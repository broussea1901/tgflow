
# TgFlow

TgFlow is a lightweight tool designed to manage and orchestrate TigerGraph loading jobs, including both streams and batches, as well as other command-based tasks. It leverages ZooKeeper for coordination, providing high availability and fault tolerance through leader-follower election mechanisms.

## Features

- **Job Management**:
  - Supports TigerGraph PSQL batch and stream jobs.
  - Executes custom command-based tasks.
  - Handles job scheduling with flexible frequency options (e.g., seconds, minutes, hours, days).
- **Cluster Monitoring**:
  - Displays the current leader with election timestamps.
  - Lists followers with rejoin timestamps.
- **High Availability**:
  - Uses ZooKeeper for distributed coordination and failover handling.
  - Leader manages all jobs, while followers wait for leadership.

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-repo/tgflow.git
   cd tgflow
   ```

2. **Set Up the Tool**:
   - Make the script executable:
     ```bash
     chmod +x tgflow.py
     ```
   - Rename it for convenience (optional):
     ```bash
     mv tgflow.py tgflow
     ```

3. **Add to PATH**:
   - Move the script to `/usr/local/bin` or `~/bin`:
     ```bash
     sudo mv tgflow /usr/local/bin/
     ```
   - Alternatively, add the repository directory to your `PATH`.

4. **Run**:
   ```bash
   tgflow --help
   ```

## Usage

### Commands

- **List All Jobs**:
  ```bash
  tgflow list-jobs
  ```
  Lists all job definitions under `/flo/jobs` in ZooKeeper.

- **Add or Update a Job**:
  ```bash
  tgflow add-job <job_name> '<job_data>'
  ```
  Example:
  ```bash
  tgflow add-job pg_batch_job '{"type": "pgsql_batch", "graph": "MyGraph", "job_name": "MyBatchJob", "frequency": "1h"}'
  ```

- **Show Cluster Status**:
  ```bash
  tgflow cluster-status
  ```
  Displays:
  - Current leader with election time.
  - List of followers with rejoin timestamps.

- **Show Leader**:
  ```bash
  tgflow show-leader
  ```

- **List Followers**:
  ```bash
  tgflow list-followers
  ```

## ZooKeeper Node Structure

### Job Nodes (`/flo/jobs/<job_name>`)

Each job node contains JSON metadata:

#### PostgreSQL Batch Jobs
```json
{
  "type": "pgsql_batch",
  "graph": "GraphName",
  "job_name": "JobName",
  "frequency": "1h"
}
```

#### PostgreSQL Stream Jobs
```json
{
  "type": "pgsql_stream",
  "graph": "GraphName",
  "job_name": "JobName"
}
```

#### Command-Based Jobs
```json
{
  "type": "command",
  "command": "/path/to/command",
  "args": ["--option1", "value1"],
  "frequency": "30m"
}
```

### Leader Node (`/flow/leader`)

The leader node contains:
```json
{
  "hostname": "leader-hostname",
  "timestamp": "2024-11-24T12:00:00Z"
}
```

### Follower Nodes (`/flow/followers/<hostname>`)

Each follower node contains:
```json
{
  "hostname": "follower-hostname",
  "timestamp": "2024-11-24T12:05:00Z"
}
```

## Advanced Configuration

### Supported Frequency Formats

- **Seconds**: `60`
- **Minutes**: `5m` (5 minutes)
- **Hours**: `1h` (1 hour)
- **Days**: `1d` (1 day)

If no suffix is provided, frequency defaults to seconds.

### Environment Variables

- **`ZOOKEEPER_CLI`**: Path to ZooKeeper CLI (default: `/path/to/zkCli.sh`).
- **`ZOOKEEPER_SERVER`**: ZooKeeper server address and port (default: `localhost:2181`).

## Developer Notes

### Extending TgFlow

- Add new job types by updating `validate_job_data` and `manage_jobs`.
- Customize leader and follower logic in `create_follower_znode` and `watch_znode`.

### Testing

- Run with mock ZooKeeper data for development:
  ```bash
  tgflow cluster-status
  ```

## License

MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Open an issue or submit a pull request on [GitHub](https://github.com/your-repo/tgflow).
