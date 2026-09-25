use crate::client::S3Client;
use crate::errors::CloudError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uintn::UintN;

fn path_to_id(path: &str, extension: &str) -> Result<UintN, CloudError> {
    let mut hex_string = String::new();
    for component in path.split('/') {
        if let Some(stripped) = component.strip_suffix(&format!(".{}", extension)) {
            hex_string.push_str(stripped);
        } else if !component.is_empty() {
            hex_string.push_str(component);
        }
    }
    Ok(UintN::from_hex_digits(&hex_string)?)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    Min,
    Max,
}

impl End {
    /// The order to try directories in: the first with anything in it wins.
    fn hex_digits(self) -> Vec<char> {
        let mut digits = vec![
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
        ];
        if self == End::Max {
            digits.reverse();
        }
        digits
    }

    fn better(self, candidate: &UintN, current: &UintN) -> bool {
        match self {
            End::Min => candidate < current,
            End::Max => candidate > current,
        }
    }
}

/// Files at a level are the ids with exactly this many digits; directories
/// hold longer ones. So the smallest id is a file here if there is one, the
/// largest is in a directory if there is one, and the two ends scan a level
/// in opposite orders.
fn find_id_recursive<'a>(
    client: &'a Arc<S3Client>,
    current_prefix: &'a str,
    extension: &'a str,
    end: End,
) -> Pin<Box<dyn Future<Output = Result<String, CloudError>> + Send + 'a>> {
    Box::pin(async move {
        let delimiter = "/";

        log::debug!("find_min_id_recursive: Listing prefix: {}", current_prefix);

        let list_result = client.list_objects(current_prefix, Some(delimiter)).await?;

        log::debug!(
            "find_min_id_recursive: Got {} objects for prefix: {}",
            list_result.contents.len(),
            current_prefix
        );

        let mut min_file: Option<(String, UintN)> = None;

        log::debug!(
            "find_min_id_recursive: Processing {} objects",
            list_result.contents.len()
        );
        for object in &list_result.contents {
            let key = &object.key;
            let relative_key = key.strip_prefix(current_prefix).unwrap_or(key);
            let relative_key = relative_key.trim_start_matches('/');

            if !relative_key.contains('/') && key.ends_with(&format!(".{}", extension)) {
                log::debug!(
                    "find_min_id_recursive: Found file at current level: {}",
                    key
                );
                if let Ok(id) = path_to_id(relative_key, extension) {
                    match &min_file {
                        Some((_, min_id)) if end.better(&id, min_id) => {
                            log::debug!(
                                "find_min_id_recursive: New min file: {} (id: {:?})",
                                relative_key,
                                id
                            );
                            min_file = Some((relative_key.to_string(), id));
                        }
                        None => {
                            log::debug!(
                                "find_min_id_recursive: First min file: {} (id: {:?})",
                                relative_key,
                                id
                            );
                            min_file = Some((relative_key.to_string(), id));
                        }
                        _ => {}
                    }
                }
            }
        }

        if end == End::Min
            && let Some((path, id)) = &min_file
        {
            log::debug!(
                "find_min_id_recursive: Found file at current level, returning early: {} (id: {:?})",
                path,
                id
            );
            return Ok(path.clone());
        }

        for hex_digit in end.hex_digits() {
            let hex_str = hex_digit.to_string();

            let mut found_dir = false;
            log::debug!(
                "find_min_id_recursive: Checking {} common prefixes for hex digit: {}",
                list_result.common_prefixes.len(),
                hex_str
            );
            for common_prefix in &list_result.common_prefixes {
                let dir_name = common_prefix
                    .prefix
                    .strip_prefix(current_prefix)
                    .unwrap_or(&common_prefix.prefix)
                    .trim_start_matches('/')
                    .trim_end_matches('/');

                log::debug!("find_min_id_recursive: Found directory: {}", dir_name);

                if dir_name == hex_str {
                    found_dir = true;
                    let subdir_prefix = &common_prefix.prefix;

                    log::debug!(
                        "find_min_id_recursive: Recursing into directory: {}",
                        subdir_prefix
                    );

                    match find_id_recursive(client, subdir_prefix, extension, end).await {
                        Ok(sub_path) => {
                            let full_path = format!("{}/{}", hex_str, sub_path);
                            return Ok(full_path);
                        }
                        Err(CloudError::NoFilesFound) => {
                            log::debug!(
                                "find_min_id_recursive: No files in directory {}, continuing",
                                hex_str
                            );
                        }
                        Err(e) => return Err(e),
                    }
                    break;
                }
            }
            if found_dir {
                break;
            }
        }

        match min_file {
            Some((path, _)) => Ok(path),
            None => Err(CloudError::NoFilesFound),
        }
    })
}

pub async fn find_min_id(
    client: &Arc<S3Client>,
    prefix: &str,
    extension: &str,
) -> Result<UintN, CloudError> {
    log::debug!(
        "find_min_id: Starting search in prefix: {} with extension: {}",
        prefix,
        extension
    );
    let relative_path = find_id_recursive(client, prefix, extension, End::Min).await?;
    log::debug!("find_min_id: Found min path: {}", relative_path);
    let id = path_to_id(&relative_path, extension)?;
    log::debug!("find_min_id: Min ID: {:?}", id);
    Ok(id)
}

/// The highest file id under `prefix`: `find_min_id`'s descent, directories
/// first and the digits reversed. One LIST per level, like it.
pub async fn find_max_id(
    client: &Arc<S3Client>,
    prefix: &str,
    extension: &str,
) -> Result<UintN, CloudError> {
    let relative_path = find_id_recursive(client, prefix, extension, End::Max).await?;
    let id = path_to_id(&relative_path, extension)?;
    log::debug!("find_max_id: Max ID: {:?}", id);
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_id() {
        let id = path_to_id("12/34/56.store", "store").unwrap();
        assert_eq!(id, UintN::from_hex_digits("123456").unwrap());

        let id = path_to_id("a/b/c/d.store", "store").unwrap();
        assert_eq!(id, UintN::from_hex_digits("abcd").unwrap());

        let id = path_to_id("12/3456.store", "store").unwrap();
        assert_eq!(id, UintN::from_hex_digits("123456").unwrap());

        let id = path_to_id("123456.store", "store").unwrap();
        assert_eq!(id, UintN::from_hex_digits("123456").unwrap());
    }
}
