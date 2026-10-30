//! Retention for L0 objects that an L1 object already covers.
//!
//! celld-original, not a Litestream port. Additive compaction
//! ([`crate::replica_compactor`]) publishes an L1 object over a run of L0
//! objects and deletes nothing, so inside one long-lived epoch the L0 prefix
//! grows by one object per transaction forever. Epoch GC removes only whole
//! superseded epochs, so it never touches the live one.
//!
//! An L0 object is removable when one L1 object's TXID range contains its
//! own. The restore planner ([`crate::replica::calc_restore_plan`]) chooses,
//! at every step, the eligible file that reaches the highest TXID, and an
//! eligible L0 object's covering L1 object is always eligible too (it starts
//! no later and ends no earlier), so a restore to the latest position never
//! needed the L0 object. LTX objects carry page images, so applying the
//! wider L1 object over a position inside its range is how restore already
//! works. What is lost is point-in-time restore to a TXID strictly inside an
//! L1 range, and only for L0 objects older than the grace.
//!
//! An object with no known upload time is kept: every uncertain fact keeps
//! the object.

use crate::client::ReplicaClient;
use crate::error::Result;
use crate::ltx::FileInfo;
use crate::TXID;
use std::time::{Duration, SystemTime};

/// The L0 objects in `l0` that one object in `l1` covers and that are at
/// least `grace` old at `now`, oldest first, at most `max`.
///
/// `l1` may be in any order and may overlap. An L0 object `f` is covered
/// when some L1 object `g` has `g.min_txid <= f.min_txid` and
/// `f.max_txid <= g.max_txid`.
pub fn covered_l0(
    l0: &[FileInfo],
    l1: &[FileInfo],
    now: SystemTime,
    grace: Duration,
    max: usize,
) -> Vec<FileInfo> {
    // Sorted by min_txid, with the running maximum of max_txid: the L1
    // object with the highest end among those that start at or before an L0
    // object's start is the one that can contain it.
    let mut starts: Vec<(TXID, TXID)> = l1.iter().map(|g| (g.min_txid, g.max_txid)).collect();
    starts.sort_unstable();
    let mut reach = Vec::with_capacity(starts.len());
    let mut best = TXID(0);
    for (_, max_txid) in &starts {
        best = best.max(*max_txid);
        reach.push(best);
    }
    let covered = |f: &FileInfo| {
        let n = starts.partition_point(|(min_txid, _)| *min_txid <= f.min_txid);
        n > 0 && reach[n - 1] >= f.max_txid
    };
    let old_enough = |f: &FileInfo| {
        f.created_at
            .and_then(|created| now.duration_since(created).ok())
            .is_some_and(|age| age >= grace)
    };
    let mut victims: Vec<FileInfo> = l0
        .iter()
        .filter(|f| f.level == 0 && f.min_txid <= f.max_txid)
        .filter(|f| covered(f) && old_enough(f))
        .cloned()
        .collect();
    victims.sort_by_key(|f| (f.min_txid, f.max_txid));
    victims.truncate(max);
    victims
}

/// What one L0 retention pass saw and did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct L0RetentionOutcome {
    /// L0 objects listed, at most the scan bound.
    pub scanned: usize,
    /// L0 objects deleted, oldest first.
    pub deleted: Vec<FileInfo>,
    /// The scan reached its bound, so older covered objects may remain for
    /// the next pass.
    pub more: bool,
}

/// Plans one pass over `client` without deleting anything: lists at most
/// `scan` of the oldest L0 objects and every L1 object, and returns the L0
/// objects [`covered_l0`] would delete, at most `max`.
pub async fn plan_l0_retention<C: ReplicaClient + ?Sized>(
    client: &C,
    now: SystemTime,
    grace: Duration,
    scan: usize,
    max: usize,
) -> Result<L0RetentionOutcome> {
    let l0 = client.ltx_files_bounded(0, TXID(0), scan).await?;
    if l0.is_empty() {
        return Ok(L0RetentionOutcome::default());
    }
    let l1 = client.ltx_files(1, TXID(0)).await?;
    let deleted = covered_l0(&l0, &l1, now, grace, max);
    Ok(L0RetentionOutcome {
        scanned: l0.len(),
        more: l0.len() >= scan || deleted.len() >= max,
        deleted,
    })
}

/// Deletes the L0 objects a [`plan_l0_retention`] pass returned.
pub async fn apply_l0_retention<C: ReplicaClient + ?Sized>(
    client: &C,
    outcome: &L0RetentionOutcome,
) -> Result<()> {
    if outcome.deleted.is_empty() {
        return Ok(());
    }
    client.delete_ltx_files(&outcome.deleted).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::calc_restore_plan;
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    const HOUR: Duration = Duration::from_secs(3600);

    fn file(level: i32, min: u64, max: u64, age: Duration, now: SystemTime) -> FileInfo {
        FileInfo {
            level,
            min_txid: TXID(min),
            max_txid: TXID(max),
            created_at: Some(now - age),
            size: 1,
            ..FileInfo::default()
        }
    }

    /// Listing and delete only: the restore planner and the retention pass
    /// use nothing else.
    #[derive(Default)]
    struct Listing(Mutex<BTreeMap<i32, Vec<FileInfo>>>);

    #[async_trait]
    impl ReplicaClient for Listing {
        async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>> {
            let mut files: Vec<FileInfo> = self
                .0
                .lock()
                .unwrap()
                .get(&level)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|f| f.min_txid >= seek)
                .collect();
            files.sort_by_key(|f| f.min_txid);
            Ok(files)
        }
        async fn open_ltx_file(&self, _: i32, _: TXID, _: TXID) -> Result<Vec<u8>> {
            unimplemented!()
        }
        async fn write_ltx_file(&self, _: i32, _: TXID, _: TXID, _: &[u8]) -> Result<FileInfo> {
            unimplemented!()
        }
        async fn delete_ltx_files(&self, files: &[FileInfo]) -> Result<()> {
            let mut levels = self.0.lock().unwrap();
            for f in files {
                if let Some(level) = levels.get_mut(&f.level) {
                    level.retain(|g| (g.min_txid, g.max_txid) != (f.min_txid, f.max_txid));
                }
            }
            Ok(())
        }
        async fn delete_all(&self) -> Result<()> {
            unimplemented!()
        }
    }

    fn ranges(files: &[FileInfo]) -> Vec<(i32, u64, u64)> {
        files
            .iter()
            .map(|f| (f.level, f.min_txid.0, f.max_txid.0))
            .collect()
    }

    #[test]
    fn only_contained_and_old_l0_objects_are_victims() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let old = 2 * HOUR;
        let l1 = vec![file(1, 1, 10, old, now), file(1, 11, 20, old, now)];
        let l0 = vec![
            file(0, 1, 1, old, now),
            file(0, 2, 5, old, now),
            // Straddles two L1 objects: neither contains it.
            file(0, 10, 11, old, now),
            file(0, 12, 12, Duration::from_secs(60), now),
            file(0, 20, 20, old, now),
            // Above every L1 object: compaction has not folded it yet.
            file(0, 21, 21, old, now),
            FileInfo {
                created_at: None,
                ..file(0, 3, 3, old, now)
            },
        ];
        let victims = covered_l0(&l0, &l1, now, HOUR, usize::MAX);
        assert_eq!(ranges(&victims), vec![(0, 1, 1), (0, 2, 5), (0, 20, 20)]);
        assert_eq!(covered_l0(&l0, &l1, now, HOUR, 2).len(), 2);
        assert!(covered_l0(&l0, &[], now, HOUR, usize::MAX).is_empty());
    }

    #[test]
    fn overlapping_l1_objects_cover_by_their_widest_reach() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let old = 2 * HOUR;
        let l1 = vec![file(1, 5, 30, old, now), file(1, 1, 8, old, now)];
        let l0 = vec![file(0, 6, 29, old, now), file(0, 2, 9, old, now)];
        // [6,29] is inside [5,30]; [2,9] starts before 5 and ends after 8.
        assert_eq!(
            ranges(&covered_l0(&l0, &l1, now, HOUR, usize::MAX)),
            vec![(0, 6, 29)]
        );
    }

    #[tokio::test]
    async fn retention_keeps_the_latest_restore_and_drops_covered_segments() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let old = 2 * HOUR;
        let listing = Listing::default();
        {
            let mut levels = listing.0.lock().unwrap();
            levels.insert(0, (1..=40).map(|t| file(0, t, t, old, now)).collect());
            levels.insert(1, vec![file(1, 1, 16, old, now), file(1, 17, 32, old, now)]);
        }
        let before = calc_restore_plan(&listing, TXID(0)).await.unwrap();
        let outcome = plan_l0_retention(&listing, now, HOUR, 1000, 1000)
            .await
            .unwrap();
        assert_eq!(outcome.scanned, 40);
        assert_eq!(outcome.deleted.len(), 32);
        assert!(!outcome.more);
        apply_l0_retention(&listing, &outcome).await.unwrap();

        let left = listing.ltx_files(0, TXID(0)).await.unwrap();
        assert_eq!(left.first().map(|f| f.min_txid), Some(TXID(33)));
        let after = calc_restore_plan(&listing, TXID(0)).await.unwrap();
        assert_eq!(ranges(&after), ranges(&before));
        assert_eq!(after.last().map(|f| f.max_txid), Some(TXID(40)));
    }

    #[tokio::test]
    async fn a_bounded_pass_reports_more_work() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let old = 2 * HOUR;
        let listing = Listing::default();
        {
            let mut levels = listing.0.lock().unwrap();
            levels.insert(0, (1..=40).map(|t| file(0, t, t, old, now)).collect());
            levels.insert(1, vec![file(1, 1, 40, old, now)]);
        }
        let outcome = plan_l0_retention(&listing, now, HOUR, 10, 1000)
            .await
            .unwrap();
        assert_eq!(outcome.deleted.len(), 10);
        assert!(outcome.more);
        // Nothing is gone until the caller applies the plan.
        assert_eq!(listing.ltx_files(0, TXID(0)).await.unwrap().len(), 40);
    }

    #[tokio::test]
    async fn young_segments_wait_for_the_grace() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let listing = Listing::default();
        {
            let mut levels = listing.0.lock().unwrap();
            levels.insert(
                0,
                (1..=4)
                    .map(|t| file(0, t, t, Duration::from_secs(5), now))
                    .collect(),
            );
            levels.insert(1, vec![file(1, 1, 4, Duration::from_secs(5), now)]);
        }
        let outcome = plan_l0_retention(&listing, now, HOUR, 100, 100)
            .await
            .unwrap();
        assert!(outcome.deleted.is_empty());
    }
}
