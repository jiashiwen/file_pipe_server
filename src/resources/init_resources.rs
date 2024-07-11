use anyhow::Result;

use super::change_taskstatus_to_stop;

pub async fn init_resources() -> Result<()> {
    change_taskstatus_to_stop()?;
    Ok(())
}
