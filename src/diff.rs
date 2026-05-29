use crate::ws::DiffHunk;
use crate::ws::DiffKind;

pub fn diff_output(old: &str, new: &str) -> Vec<DiffHunk> {
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();

    let mut hunks = Vec::new();

    let mut i = 0;
    let mut j = 0;

    while i < old_lines.len() || j < new_lines.len() {
        match (old_lines.get(i), new_lines.get(j)) {
            (Some(o), Some(n)) if o == n => {
                hunks.push(DiffHunk {
                    kind: DiffKind::Equal,
                    content: n.to_string(),
                });

                i += 1;
                j += 1;
            }
            (Some(o), Some(n)) => {
                hunks.push(DiffHunk {
                    kind: DiffKind::Removed,
                    content: o.to_string(),
                });
                hunks.push(DiffHunk {
                    kind: DiffKind::Added,
                    content: n.to_string(),
                });
                i += 1;
                j += 1;
            }
            (Some(o), None) => {
                hunks.push(DiffHunk {
                    kind: DiffKind::Removed,
                    content: o.to_string(),
                });
                i += 1;
            }
            (None, Some(n)) => {
                hunks.push(DiffHunk {
                    kind: DiffKind::Added,
                    content: n.to_string(),
                });
                j += 1;
            }
            (None, None) => break,
        }
    }

    hunks
}
