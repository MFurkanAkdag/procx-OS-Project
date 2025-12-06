
> Note: This README is written in English for project submission.  

# ProcX – Advanced Process Management System

ProcX is a multi-instance, terminal-based process management system for Linux.  
It allows users to start, list and terminate programs in the background (similar to `nohup`), and keep a **shared, synchronized process list** across multiple terminal sessions.   

Each ProcX instance:

- Shares process information via **POSIX shared memory**
- Protects critical sections with a **POSIX named semaphore**
- Exchanges notifications over **POSIX message queues**
- Runs **two worker threads**:
  - A **monitor thread** that periodically checks process status
  - An **IPC listener thread** that prints notifications from other instances   

This project is implemented in a single C source file (`procx.c`) with a `Makefile` and this `README.md`, as required by the assignment.   

---

## 1. Project Goals & Learning Outcomes

The project is designed to practice:   

- Process creation and management (`fork`, `execvp`)
- Thread programming (`pthread`)
- Inter-process communication (shared memory, message queues)
- Synchronization (semaphores)
- Signal management (`SIGCHLD`, `SIGINT`, `SIGTERM`)
- Creating daemon-like processes (`setsid`, double fork)

After completing the project, you demonstrate that you can:

- Create and manage processes with `fork()`/`execvp()`
- Share state between processes using POSIX shared memory
- Avoid race conditions via semaphores
- Use message queues to notify other instances
- Manage signals and prevent zombie processes
- Design and implement a small concurrent system architecture

---

## 2. Project Structure

The project follows the required directory layout:   

```text
procx/
├── procx.c      # Main source file (single C file)
├── Makefile     # Build configuration
└── README.md    # Project description (this file)
````

* **`procx.c`**
  Contains all logic: data structures, IPC initialization, process creation, monitor & IPC threads, and the CLI menu.

* **`Makefile`**
  Compiles `procx.c` into the `procx` executable (uses `gcc`, `-pthread`, and links POSIX IPC as needed).

* **`README.md`**
  Explains how to build, run, and test the project, and documents the architecture and design decisions.

---

## 3. Requirements

* **Operating System:** Linux (tested on a POSIX-compliant environment)
* **Compiler:** `gcc` with POSIX support
* **Libraries / APIs used:**

  * POSIX shared memory: `shm_open`, `mmap`, `ftruncate`, `shm_unlink`
  * POSIX named semaphores: `sem_open`, `sem_wait`, `sem_post`, `sem_close`, `sem_unlink`
  * POSIX message queues: `mq_open`, `mq_send`, `mq_timedreceive`, `mq_close`, `mq_unlink`
  * Threads: `pthread_create`, `pthread_join`
  * Process APIs: `fork`, `execvp`, `waitpid`
  * Signals: `sigaction` for `SIGINT`, `SIGTERM`, `SIGCHLD`

---

## 4. Build & Run

### 4.1. Build

From inside the `procx/` directory:

```bash
make
```

This should produce an executable named:

```bash
./procx
```

### 4.2. Run

Run ProcX from a **normal Linux terminal** (recommended, e.g., GNOME Terminal, xterm, WSL shell):

```bash
./procx
```

> **Important note:**
> Some IDE or integrated terminals (like VS Code’s built-in terminal or “Run task”) may send extra signals to the entire process tree when the run is stopped. This can kill even detached processes. For correct behavior, run ProcX directly from a standard terminal and exit via the menu (`0`).

When started, ProcX prints an ASCII banner and shows the main menu:

```text
╔════════════════════════════════════╗
║ ProcX v1.0                         ║
╠════════════════════════════════════╣
║ 1. Run a new program               ║
║ 2. List running programs           ║
║ 3. Terminate a program             ║
║ 0. Exit                            ║
╚════════════════════════════════════╝
Your choice:
```

---

## 5. Usage

### 5.1. 1 – Run a new program

Choose **1** from the main menu:

```text
Your choice: 1
Enter the command to run: sleep 100
Choose running mode (0: Attached, 1: Detached): 1
[SUCCESS] Process started: PID 12345 (DETACHED)
```

* **Command:** any valid program, e.g. `sleep 100`, `firefox`, etc.
* **Mode:**

  * `0` – **Attached**

    * Process is considered “owned” by that ProcX instance and will be terminated when that instance exits.
  * `1` – **Detached**

    * Process is started via `fork` + `execvp` and daemonized using `setsid()` and a double fork.
    * It **continues running** even if the owner ProcX instance exits.

After creation:

* The process is added to **shared memory**.
* A notification is sent to all other instances via **message queues**:
  Other terminals running ProcX will see:

  ```text
  [IPC] New process started: PID <pid> (by instance <owner_pid>)
  ```

### 5.2. 2 – List running programs

Choose **2** from the main menu to display the shared process list:

```text
╔═══════════════════════════════════════════════════════════════════════════╗
║                           RUNNING PROGRAMS                                ║
╠═══════════════════════════════════════════════════════════════════════════╣
║ PID     │ Command              │ Mode       │ Owner    │ Time     ║
╠═══════════════════════════════════════════════════════════════════════════╣
║ 12345   │ sleep 100            │ Detached   │ 10000    │    15s   ║
║ 12346   │ firefox              │ Attached   │ 10001    │    30s   ║
╚═══════════════════════════════════════════════════════════════════════════╝
Total: 2 process(es)
```

Each row shows:

* **PID** – process ID
* **Command** – the original command (truncated for display)
* **Mode** – `Attached` or `Detached`
* **Owner** – PID of the ProcX instance that currently owns the record
* **Time** – elapsed runtime in seconds since the process was started

Before printing, the implementation:

1. Scans the table for **stale PIDs** (processes that no longer exist),
2. Cleans up their records from shared memory,
3. Broadcasts a “terminated” IPC message,
4. Then prints the remaining active processes.

### 5.3. 3 – Terminate a program

Choose **3** to terminate a process:

```text
Your choice: 3
Process PID to terminate: 12345
[INFO] Signal SIGTERM sent to Process 12345
```

Behavior:

1. ProcX looks up the PID in shared memory.
2. It verifies that:

   * The process still exists, and
   * The `/proc/<pid>/cmdline` matches the stored command (to avoid killing an unrelated process if PIDs were reused).
3. Sends `SIGTERM` with `kill(pid, SIGTERM)`.
4. If the process is already gone, the record is removed and a termination notification is broadcast.
5. If termination is successful, the monitor thread eventually reaps the child (for attached processes) and removes the record, also sending an IPC notification:

   ```text
   [MONITOR] Process 12345 was terminated (exit code: 0)
   [IPC] Process terminated: PID 12345 (by instance <pid>)
   ```

If PID is invalid or not found in the table, an error message is printed instead of sending a signal.

### 5.4. 0 – Exit

Choose **0** to exit ProcX cleanly:

* Sets a shutdown flag so both worker threads stop.
* Calls `pthread_join` to wait for:

  * Monitor thread
  * IPC listener thread
* Calls `proc_cleanup_attached()`:

  * Sends `SIGTERM` to **all attached processes** owned by this instance.
  * Detached processes are **not** killed and are left running.
* Cleans IPC resources:

  * Closes and unmaps shared memory
  * Closes semaphore and message queue
  * Unlinks per-instance MQ

Other instances remain alive, and the shared process list keeps being managed by them.

---

## 6. System Architecture

### 6.1. Data Structures

The core structures (stored in shared memory) are:

```c
typedef struct {
    pid_t pid;                 // Process ID
    pid_t owner_pid;           // PID of ProcX instance that owns this record
    char command[MAX_CMD_LEN]; // Original command
    ProcessMode mode;          // MODE_ATTACHED or MODE_DETACHED
    ProcessStatus status;      // STATUS_RUNNING or STATUS_TERMINATED
    time_t start_time;         // Start timestamp
    int is_active;             // 1 if this slot is in use
} ProcessInfo;

typedef struct {
    ProcessInfo processes[MAX_PROCESSES];   // Up to 50 processes
    int process_count;                      // Active process count
    int instance_count;                     // (Optional) global instance counter
    pid_t active_instances[MAX_INSTANCES];  // PIDs of active ProcX instances
    int active_instance_count;              // Number of active instances
} SharedData;
```

* `ProcessInfo` holds per-process metadata.
* `SharedData` is a fixed-size shared array plus counters.

All access to `SharedData` is protected by a named semaphore (`/procx_sem`).

### 6.2. IPC Mechanisms

The project uses three POSIX IPC mechanisms:

| Mechanism      | Name pattern      | Purpose                           |
| -------------- | ----------------- | --------------------------------- |
| Shared memory  | `/procx_shm`      | Global process table              |
| Semaphore      | `/procx_sem`      | Mutual exclusion on shared memory |
| Message queues | `/procx_mq_<pid>` | Per-instance notification channel |

* Each instance opens the same shared memory + semaphore.
* Each instance creates its own **message queue** named using its own PID (e.g. `/procx_mq_12345`).
* When a process starts or terminates, an `IPCMessage` is sent to **all other instances’ queues**:

  ```c
  typedef struct {
      long msg_type;
      int command;       // IPC_CMD_PROCESS_STARTED / IPC_CMD_PROCESS_TERMINATED
      pid_t sender_pid;  // ProcX sender
      pid_t target_pid;  // Process PID
  } IPCMessage;
  ```

### 6.3. Threads

Each ProcX instance creates three threads:

1. **Main thread**

   * Handles the menu and user interactions.
   * Invokes functions like `proc_start_interactive`, `proc_list_all`, `proc_terminate_interactive`, and `proc_cleanup_attached`.

2. **Monitor thread**

   * Periodically (every `MONITOR_INTERVAL_SEC` seconds) calls `monitor_scan_once(my_pid)`.
   * For each active record:

     * If the **owner is dead**:

       * Attached: removes the record and broadcasts termination.
       * Detached: **adopts** the process (changes `owner_pid` to the current instance and logs:

         ```text
         [MONITOR] Adopting detached process <pid> (previous owner <old_pid>).
         ```
     * Checks whether the process is still valid (exists and matches the stored command).

       * If not valid, removes the record and sends an IPC termination message.
     * For **attached processes that this instance owns**, calls `waitpid(pid, &status, WNOHANG)` to reap them and log how they exited.

3. **IPC listener thread**

   * Uses `mq_timedreceive` with a 1-second timeout to read messages from its own message queue.
   * For each message:

     * If `IPC_CMD_PROCESS_STARTED`, prints:

       ```text
       [IPC] New process started: PID <target_pid> (by instance <sender_pid>)
       ```
     * If `IPC_CMD_PROCESS_TERMINATED`, prints:

       ```text
       [IPC] Process terminated: PID <target_pid> (by instance <sender_pid>)
       ```
   * Re-prints `Your choice:` prompt after each message so the menu remains user-friendly in multi-instance scenarios.

### 6.4. Attached vs Detached Behavior

* **Attached mode (`MODE_ATTACHED`)**

  * ProcX remains the parent of the child process.
  * The monitor thread uses `waitpid(..., WNOHANG)` to detect termination and reap zombies.
  * On exit (`choice = 0`), ProcX:

    * Sends `SIGTERM` to all attached processes it owns.
    * Leaves detached processes running.

* **Detached mode (`MODE_DETACHED`)**

  * Uses a **double fork** and `setsid()` to detach from the controlling terminal and create a new session.
  * Standard streams are typically redirected to `/dev/null`, making the process “daemon-like”.
  * The detached process:

    * Continues running when the owner ProcX exits.
    * Can later be **adopted** by another ProcX instance if the original owner dies.
    * Remains visible in the shared process list, and can still be terminated by any ProcX instance via PID.

---

## 7. Test Scenarios

The implementation supports all five required test scenarios from the project description.

Below is a brief summary (see the project PDF or your separate test document for full details):

1. **Test 1 – Single Instance: Process Start and List**

   * Start `./procx` in one terminal.
   * Create an attached and a detached process (e.g. `sleep`).
   * Use “List running programs” to verify they appear correctly.

2. **Test 2 – Multi-Instance: IPC**

   * Run `./procx` in **two terminals**.
   * Start a process in Terminal 1.
   * Verify Terminal 2 prints:

     ```text
     [IPC] New process started: PID <pid> (by instance <pid>)
     ```
   * Listing in both terminals should show the same processes.

3. **Test 3 – Attached vs Detached Mode**

   * Start one attached and one detached process in Terminal 1.
   * Exit ProcX (choice `0`).
   * In another terminal, check:

     * Attached process is gone.
     * Detached process is still running (`ps` command) and visible to a new ProcX instance.

4. **Test 4 – Process Termination**

   * Start a long-running process.
   * Use “Terminate a program” with the process PID.
   * Check that:

     * The process is gone from `ps`.
     * The process is removed from the ProcX list.
     * Monitor + IPC messages are printed.

5. **Test 5 – Monitor Thread Test**

   * Start a short-lived process (e.g. `sleep 5`).
   * Wait without interacting.
   * After it finishes, the monitor thread should log its termination and remove the record automatically.

---

## 8. Known Limitations / Notes

* **IDE / integrated terminals:**
  Some environments kill all child processes when the run is stopped, which can break the “detached” guarantee. For grading and accurate behavior, use a **regular Linux terminal** and exit ProcX via the menu.

* **Shared IPC cleanup after crash:**
  The code attempts to clean up and detect stale data on startup (removing dead processes and adopting detached ones whose owners no longer exist). However, if the program is killed with `kill -9` at the wrong time, some IPC objects may be left behind and need manual cleanup (e.g., `ls /dev/mqueue` or `ipcs`).

* **Process matching:**
  To avoid accidentally killing an unrelated process after PID reuse, the implementation compares the current `/proc/<pid>/cmdline` with the stored command string. If they differ, it cleans the record instead of sending a signal.

---
