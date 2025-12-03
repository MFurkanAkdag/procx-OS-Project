/*
 * ProcX – Advanced Process Management System
 * Multi-instance CLI process manager built with POSIX shared memory, semaphores,
 * message queues, and worker threads for monitoring and IPC listening.
 * Author: <Your Name> (<Student ID>)
 */
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <mqueue.h>
#include <pthread.h>
#include <time.h>

/* Config */
#define SHM_NAME            "/procx_shm"
#define SEM_NAME            "/procx_sem"
#define MQ_NAME             "/procx_mq"
#define MAX_PROCESSES       50
#define MAX_CMD_LEN         256
#define MAX_ARGS            64
#define MQ_MAX_MESSAGES     10
#define MQ_MSG_SIZE         sizeof(IPCMessage)
#define MONITOR_INTERVAL_SEC 2
#define PROCX_VERSION       "1.0"
#define IPC_PERMISSIONS     0666

/* Types */
typedef enum {
    MODE_ATTACHED = 0,
    MODE_DETACHED = 1
} ProcessMode;

typedef enum {
    STATUS_RUNNING = 0,
    STATUS_TERMINATED = 1
} ProcessStatus;

typedef enum {
    IPC_CMD_PROCESS_STARTED = 1,
    IPC_CMD_PROCESS_TERMINATED = 2
} IPCCommand;

typedef struct {
    pid_t pid;
    pid_t owner_pid;
    char command[MAX_CMD_LEN];
    ProcessMode mode;
    ProcessStatus status;
    time_t start_time;
    int is_active;
} ProcessInfo;

typedef struct {
    ProcessInfo processes[MAX_PROCESSES];
    int process_count;
    int instance_count;
} SharedData;

typedef struct {
    long msg_type;
    int command;
    pid_t sender_pid;
    pid_t target_pid;
} IPCMessage;

#define MODE_TO_STRING(mode) ((mode) == MODE_DETACHED ? "Detached" : "Attached")
#define STATUS_TO_STRING(status) ((status) == STATUS_RUNNING ? "Running" : "Terminated")

/* Forward declarations */
static void monitor_scan_once(pid_t my_pid);
static void cleanup_stale_processes_on_startup(void);
static int process_matches_command(pid_t pid, const char *expected_cmd);
void ipc_lock(void);
void ipc_unlock(void);
int mq_send_notification(IPCCommand cmd, pid_t target_pid);
void* thread_monitor_func(void *arg);
void* thread_listener_func(void *arg);
static int proc_is_alive(pid_t pid);

/* Globals */
SharedData *g_shared_data = NULL;
sem_t *g_semaphore = NULL;
mqd_t g_msg_queue = (mqd_t)-1;
static int g_shm_fd = -1;
volatile sig_atomic_t g_running = 1;
static pthread_t g_monitor_thread;
static pthread_t g_listener_thread;
static int g_threads_started = 0;

/* IPC helpers */
static int init_shared_memory(void) {
    int is_new = 0;

    if (g_semaphore == NULL) {
        fprintf(stderr, "[IPC] Semaphore must be initialized before shared memory.\n");
        return -1;
    }

    ipc_lock();

    g_shm_fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, IPC_PERMISSIONS);
    if (g_shm_fd == -1) {
        if (errno == EEXIST) {
            g_shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, IPC_PERMISSIONS);
            if (g_shm_fd == -1) {
                perror("shm_open");
                ipc_unlock();
                return -1;
            }
        } else {
            perror("shm_open");
            ipc_unlock();
            return -1;
        }
    } else {
        is_new = 1;
    }

    if (ftruncate(g_shm_fd, sizeof(SharedData)) == -1) {
        perror("ftruncate");
        if (is_new) {
            shm_unlink(SHM_NAME);
        }
        close(g_shm_fd);
        g_shm_fd = -1;
        ipc_unlock();
        return -1;
    }

    g_shared_data = mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE,
                         MAP_SHARED, g_shm_fd, 0);
    if (g_shared_data == MAP_FAILED) {
        perror("mmap");
        if (is_new) {
            shm_unlink(SHM_NAME);
        }
        close(g_shm_fd);
        g_shm_fd = -1;
        g_shared_data = NULL;
        ipc_unlock();
        return -1;
    }

    if (is_new) {
        memset(g_shared_data, 0, sizeof(SharedData));
        g_shared_data->process_count = 0;
        g_shared_data->instance_count = 1;
    } else {
        g_shared_data->instance_count++;
    }

    ipc_unlock();
    return 0;
}

static int init_semaphore(void) {
    g_semaphore = sem_open(SEM_NAME, O_CREAT, IPC_PERMISSIONS, 1);
    if (g_semaphore == SEM_FAILED) {
        perror("sem_open");
        g_semaphore = NULL;
        return -1;
    }
    return 0;
}

static int init_message_queue(void) {
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_maxmsg = MQ_MAX_MESSAGES;
    attr.mq_msgsize = sizeof(IPCMessage);

    g_msg_queue = mq_open(MQ_NAME, O_CREAT | O_RDWR, IPC_PERMISSIONS, &attr);
    if (g_msg_queue == (mqd_t)-1) {
        perror("mq_open");
        return -1;
    }
    return 0;
}

int ipc_init_all(void) {
    printf("[IPC] Initializing IPC resources...\n");
    if (init_semaphore() == -1) {
        return -1;
    }
    printf("[IPC] Semaphore initialized: %s\n", SEM_NAME);
    if (init_shared_memory() == -1) {
        sem_close(g_semaphore);
        g_semaphore = NULL;
        return -1;
    }
    printf("[IPC] Shared Memory initialized: %s\n", SHM_NAME);
    if (init_message_queue() == -1) {
        munmap(g_shared_data, sizeof(SharedData));
        g_shared_data = NULL;
        close(g_shm_fd);
        g_shm_fd = -1;
        sem_close(g_semaphore);
        g_semaphore = NULL;
        return -1;
    }
    printf("[IPC] Message Queue initialized: %s\n", MQ_NAME);
    printf("[IPC] All IPC resources ready.\n");
    return 0;
}

void ipc_cleanup_all(int unlink_resources) {
    int unlink_ipc = 0;

    printf("[IPC] Cleaning up IPC resources...\n");
    if (g_shared_data != NULL && g_semaphore != NULL) {
        ipc_lock();
        if (g_shared_data->instance_count > 0) {
            g_shared_data->instance_count--;
        }
        if (g_shared_data->instance_count == 0) {
            unlink_ipc = 1;
        }
        ipc_unlock();
    } else if (unlink_resources) {
        unlink_ipc = 1;
    }

    if (g_shared_data != NULL) {
        munmap(g_shared_data, sizeof(SharedData));
        g_shared_data = NULL;
    }
    if (g_shm_fd != -1) {
        close(g_shm_fd);
        g_shm_fd = -1;
    }
    if (g_msg_queue != (mqd_t)-1) {
        mq_close(g_msg_queue);
        g_msg_queue = (mqd_t)-1;
    }
    if (g_semaphore != NULL) {
        sem_close(g_semaphore);
        g_semaphore = NULL;
    }
    if (unlink_ipc) {
        shm_unlink(SHM_NAME);
        sem_unlink(SEM_NAME);
        mq_unlink(MQ_NAME);
        printf("[IPC] IPC resources unlinked.\n");
    }
    printf("[IPC] Cleanup complete.\n");
}

void ipc_lock(void) {
    if (g_semaphore != NULL) {
        sem_wait(g_semaphore);
    }
}

void ipc_unlock(void) {
    if (g_semaphore != NULL) {
        sem_post(g_semaphore);
    }
}

int shm_find_empty_slot(void) {
    if (g_shared_data == NULL) {
        return -1;
    }
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (!g_shared_data->processes[i].is_active) {
            return i;
        }
    }
    return -1;
}

int shm_find_by_pid(pid_t pid) {
    if (g_shared_data == NULL) {
        return -1;
    }
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (g_shared_data->processes[i].is_active &&
            g_shared_data->processes[i].pid == pid) {
            return i;
        }
    }
    return -1;
}

int shm_add_process(pid_t pid, pid_t owner_pid, const char *command, ProcessMode mode) {
    if (g_shared_data == NULL) {
        return -1;
    }

    int slot = shm_find_empty_slot();
    if (slot == -1) {
        return -1;
    }

    ProcessInfo *proc = &g_shared_data->processes[slot];
    proc->pid = pid;
    proc->owner_pid = owner_pid;
    strncpy(proc->command, command, MAX_CMD_LEN - 1);
    proc->command[MAX_CMD_LEN - 1] = '\0';
    proc->mode = mode;
    proc->status = STATUS_RUNNING;
    proc->start_time = time(NULL);
    proc->is_active = 1;
    g_shared_data->process_count++;
    return slot;
}

void shm_remove_process(int index) {
    if (g_shared_data == NULL) {
        return;
    }
    if (index < 0 || index >= MAX_PROCESSES) {
        return;
    }
    if (g_shared_data->processes[index].is_active) {
        g_shared_data->processes[index].is_active = 0;
        g_shared_data->processes[index].status = STATUS_TERMINATED;
        g_shared_data->process_count--;
    }
}

int shm_get_process_count(void) {
    if (g_shared_data == NULL) {
        return 0;
    }
    return g_shared_data->process_count;
}

int mq_send_notification(IPCCommand cmd, pid_t target_pid) {
    if (g_msg_queue == (mqd_t)-1) {
        return -1;
    }

    IPCMessage msg;
    msg.msg_type = 1;
    msg.command = cmd;
    msg.sender_pid = getpid();
    msg.target_pid = target_pid;

    if (mq_send(g_msg_queue, (const char *)&msg, sizeof(IPCMessage), 0) == -1) {
        if (errno != EAGAIN) {
            perror("mq_send");
        }
        return -1;
    }
    return 0;
}

int mq_receive_message(IPCMessage *msg) {
    if (g_msg_queue == (mqd_t)-1 || msg == NULL) {
        return -1;
    }
    ssize_t bytes = mq_receive(g_msg_queue, (char *)msg, sizeof(IPCMessage), NULL);
    if (bytes == -1) {
        if (errno == EAGAIN) {
            return 0;
        }
        return -1;
    }
    return 1;
}

int mq_receive_message_blocking(IPCMessage *msg) {
    if (g_msg_queue == (mqd_t)-1 || msg == NULL) {
        return -1;
    }
    ssize_t bytes = mq_receive(g_msg_queue, (char *)msg, sizeof(IPCMessage), NULL);
    if (bytes == -1) {
        if (errno == EINTR) {
            return 0;
        }
        return -1;
    }
    return 1;
}

/* Signal handling */
static void handler_sigint(int sig) {
    (void)sig;
    const char msg[] = "\n[SIGNAL] SIGINT received. Shutting down...\n";
    write(STDOUT_FILENO, msg, sizeof(msg) - 1);
    g_running = 0;
}

static void handler_sigterm(int sig) {
    (void)sig;
    const char msg[] = "\n[SIGNAL] SIGTERM received. Shutting down...\n";
    write(STDOUT_FILENO, msg, sizeof(msg) - 1);
    g_running = 0;
}

static void handler_sigchld(int sig) {
    (void)sig;
    /* Reap all zombie children to prevent accumulation */
    int saved_errno = errno;
    while (waitpid(-1, NULL, WNOHANG) > 0) {
        /* Keep reaping until no more zombies */
    }
    errno = saved_errno;
}

int setup_signal_handlers(void) {
    struct sigaction sa_int, sa_term, sa_chld;

    memset(&sa_int, 0, sizeof(sa_int));
    sa_int.sa_handler = handler_sigint;
    sigemptyset(&sa_int.sa_mask);
    if (sigaction(SIGINT, &sa_int, NULL) == -1) {
        perror("sigaction SIGINT");
        return -1;
    }

    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = handler_sigterm;
    sigemptyset(&sa_term.sa_mask);
    if (sigaction(SIGTERM, &sa_term, NULL) == -1) {
        perror("sigaction SIGTERM");
        return -1;
    }

    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = handler_sigchld;
    sigemptyset(&sa_chld.sa_mask);
    sa_chld.sa_flags = SA_NOCLDSTOP;
    if (sigaction(SIGCHLD, &sa_chld, NULL) == -1) {
        perror("sigaction SIGCHLD");
        return -1;
    }

    return 0;
}

int is_running(void) {
    return g_running;
}

void request_shutdown(void) {
    g_running = 0;
}

/* Performs a single iteration of process monitoring: existence check for all, waitpid for local attached children. */
static void monitor_scan_once(pid_t my_pid) {
    if (g_shared_data == NULL || g_semaphore == NULL) {
        return;
    }

    ipc_lock();
    for (int i = 0; i < MAX_PROCESSES; i++) {
        ProcessInfo *proc = &g_shared_data->processes[i];
        if (!proc->is_active) {
            continue;
        }

        pid_t current_pid = proc->pid;
        int is_owner = (proc->owner_pid == my_pid);
        int is_detached = (proc->mode == MODE_DETACHED);

        if (kill(proc->owner_pid, 0) == -1 && errno == ESRCH) {
            printf("\n[MONITOR] Owner %d no longer exists, removing orphaned record for PID %d.\n",
                   proc->owner_pid, current_pid);
            proc->is_active = 0;
            proc->status = STATUS_TERMINATED;
            if (g_shared_data->process_count > 0) {
                g_shared_data->process_count--;
            }
            ipc_unlock();
            mq_send_notification(IPC_CMD_PROCESS_TERMINATED, current_pid);
            printf("Your choice: ");
            fflush(stdout);
            ipc_lock();
            continue;
        }

        int process_valid = 0;
        if (kill(current_pid, 0) == 0) {
            process_valid = process_matches_command(current_pid, proc->command);
        } else if (errno == ESRCH) {
            process_valid = 0;
        } else {
            process_valid = process_matches_command(current_pid, proc->command);
        }

        if (!process_valid) {
            printf("\n[MONITOR] Process %d no longer valid, cleaning up.\n", current_pid);
            proc->is_active = 0;
            proc->status = STATUS_TERMINATED;
            if (g_shared_data->process_count > 0) {
                g_shared_data->process_count--;
            }
            ipc_unlock();
            mq_send_notification(IPC_CMD_PROCESS_TERMINATED, current_pid);
            printf("Your choice: ");
            fflush(stdout);
            ipc_lock();
            continue;
        }

        if (is_owner && !is_detached) {
            int status;
            pid_t result = waitpid(current_pid, &status, WNOHANG);
            if (result > 0) {
                printf("\n[MONITOR] Process %d was terminated", current_pid);
                if (WIFEXITED(status)) {
                    printf(" (exit code: %d)\n", WEXITSTATUS(status));
                } else if (WIFSIGNALED(status)) {
                    printf(" (killed by signal: %d)\n", WTERMSIG(status));
                } else {
                    printf("\n");
                }
                proc->is_active = 0;
                proc->status = STATUS_TERMINATED;
                if (g_shared_data->process_count > 0) {
                    g_shared_data->process_count--;
                }
                ipc_unlock();
                mq_send_notification(IPC_CMD_PROCESS_TERMINATED, current_pid);
                printf("Your choice: ");
                fflush(stdout);
                ipc_lock();
            }
        }
    }
    ipc_unlock();
}

/* Thread management */
/* Worker thread that periodically triggers monitor_scan_once for all tracked processes. */
void* thread_monitor_func(void *arg) {
    (void)arg;
    pid_t my_pid = getpid();
    printf("[MONITOR] Thread started. Checking every %d seconds.\n", MONITOR_INTERVAL_SEC);

    while (is_running()) {
        sleep(MONITOR_INTERVAL_SEC);
        if (!is_running()) {
            break;
        }
        monitor_scan_once(my_pid);
    }

    printf("[MONITOR] Thread stopping.\n");
    return NULL;
}

/* Worker thread that uses timed receive on the MQ and prints inter-instance notifications. */
void* thread_listener_func(void *arg) {
    (void)arg;
    pid_t my_pid = getpid();
    IPCMessage msg;

    printf("[IPC LISTENER] Thread started. Listening for messages.\n");

    while (is_running()) {
        /* Use timed receive with 1 second timeout for graceful shutdown */
        struct timespec timeout;
        clock_gettime(CLOCK_REALTIME, &timeout);
        timeout.tv_sec += 1;

        ssize_t bytes = mq_timedreceive(g_msg_queue, (char *)&msg, sizeof(IPCMessage), NULL, &timeout);
        if (bytes == -1) {
            if (errno == ETIMEDOUT) {
                /* Timeout, check if we should continue */
                continue;
            }
            if (errno == EINTR) {
                /* Interrupted by signal */
                continue;
            }
            if (!is_running()) {
                break;
            }
            continue;
        }

        if (msg.sender_pid == my_pid) {
            continue;
        }

        switch (msg.command) {
            case IPC_CMD_PROCESS_STARTED:
                printf("\n[IPC] New process started: PID %d (by instance %d)\n",
                       msg.target_pid, msg.sender_pid);
                break;
            case IPC_CMD_PROCESS_TERMINATED:
                printf("\n[IPC] Process terminated: PID %d (by instance %d)\n",
                       msg.target_pid, msg.sender_pid);
                break;
            default:
                printf("\n[IPC] Unknown message type: %d\n", msg.command);
                break;
        }
        printf("Your choice: ");
        fflush(stdout);
    }

    printf("[IPC LISTENER] Thread stopping.\n");
    return NULL;
}

int threads_start_all(void) {
    int ret;
    printf("[THREADS] Starting worker threads...\n");
    ret = pthread_create(&g_monitor_thread, NULL, thread_monitor_func, NULL);
    if (ret != 0) {
        fprintf(stderr, "Failed to create monitor thread: %s\n", strerror(ret));
        return -1;
    }
    ret = pthread_create(&g_listener_thread, NULL, thread_listener_func, NULL);
    if (ret != 0) {
        fprintf(stderr, "Failed to create listener thread: %s\n", strerror(ret));
        pthread_cancel(g_monitor_thread);
        pthread_join(g_monitor_thread, NULL);
        return -1;
    }
    g_threads_started = 1;
    printf("[THREADS] All worker threads started.\n");
    return 0;
}

void threads_stop_all(void) {
    if (!g_threads_started) {
        return;
    }
    printf("\n[THREADS] Stopping worker threads...\n");
    pthread_cancel(g_listener_thread);
    printf("[THREADS] Waiting for monitor thread...\n");
    pthread_join(g_monitor_thread, NULL);
    printf("[THREADS] Waiting for listener thread...\n");
    pthread_join(g_listener_thread, NULL);
    g_threads_started = 0;
    printf("[THREADS] All threads stopped.\n");
}

int threads_are_running(void) {
    return g_threads_started;
}

/* Process management */
static void trim_newline(char *str) {
    size_t len = strlen(str);
    if (len > 0 && str[len - 1] == '\n') {
        str[len - 1] = '\0';
    }
}

static void clear_stdin(void) {
    int c;
    while ((c = getchar()) != '\n' && c != EOF) {
    }
}

static const char* mode_str(ProcessMode mode) {
    return (mode == MODE_DETACHED) ? "Detached" : "Attached";
}

int proc_parse_command(char *input, char **args, int max_args) {
    int count = 0;
    char *token = strtok(input, " \t\n");
    while (token != NULL && count < max_args - 1) {
        args[count++] = token;
        token = strtok(NULL, " \t\n");
    }
    args[count] = NULL;
    return count;
}

/* Launches a new process via fork/execvp, records it in shared memory, and notifies peers. */
pid_t proc_start(const char *command, ProcessMode mode) {
    if (command == NULL || strlen(command) == 0) {
        printf("[ERROR] Empty command!\n");
        return -1;
    }

    char cmd_copy[MAX_CMD_LEN];
    strncpy(cmd_copy, command, MAX_CMD_LEN - 1);
    cmd_copy[MAX_CMD_LEN - 1] = '\0';

    char *args[MAX_ARGS];
    int arg_count = proc_parse_command(cmd_copy, args, MAX_ARGS);
    if (arg_count == 0) {
        printf("[ERROR] Invalid command!\n");
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }

    if (pid == 0) {
        if (mode == MODE_DETACHED && setsid() == -1) {
            perror("setsid");
        }
        execvp(args[0], args);
        perror("execvp");
        _exit(127);
    }

    char original_cmd[MAX_CMD_LEN];
    strncpy(original_cmd, command, MAX_CMD_LEN - 1);
    original_cmd[MAX_CMD_LEN - 1] = '\0';

    ipc_lock();
    int slot = shm_add_process(pid, getpid(), original_cmd, mode);
    ipc_unlock();

    if (slot < 0) {
        printf("[ERROR] Process list is full! Killing child...\n");
        kill(pid, SIGTERM);
        return -1;
    }

    mq_send_notification(IPC_CMD_PROCESS_STARTED, pid);
    return pid;
}

void proc_start_interactive(void) {
    char command[MAX_CMD_LEN];
    int mode_input;

    printf("Enter the command to run: ");
    if (fgets(command, MAX_CMD_LEN, stdin) == NULL) {
        printf("[ERROR] Failed to read command!\n");
        return;
    }
    trim_newline(command);
    if (strlen(command) == 0) {
        printf("[ERROR] Empty command!\n");
        return;
    }

    printf("Choose running mode (0: Attached, 1: Detached): ");
    if (scanf("%d", &mode_input) != 1) {
        printf("[ERROR] Invalid mode!\n");
        clear_stdin();
        return;
    }
    clear_stdin();

    if (mode_input != 0 && mode_input != 1) {
        printf("[ERROR] Mode must be 0 or 1!\n");
        return;
    }

    ProcessMode mode = (mode_input == 1) ? MODE_DETACHED : MODE_ATTACHED;
    pid_t pid = proc_start(command, mode);
    if (pid > 0) {
        printf("[SUCCESS] Process started: PID %d (%s)\n", pid, mode_str(mode));
    }
}

/* Issues SIGTERM to the requested PID, waits for termination, escalates if needed, and cleans up shared memory. */
int proc_terminate(pid_t pid) {
    int index;
    char expected_cmd[MAX_CMD_LEN];

    ipc_lock();
    index = shm_find_by_pid(pid);
    if (index < 0) {
        ipc_unlock();
        return -1;
    }

    ProcessInfo *proc = &g_shared_data->processes[index];
    strncpy(expected_cmd, proc->command, MAX_CMD_LEN - 1);
    expected_cmd[MAX_CMD_LEN - 1] = '\0';

    int process_valid = 0;
    int probe = kill(pid, 0);
    if (probe == 0) {
        process_valid = process_matches_command(pid, expected_cmd);
    } else if (errno == ESRCH) {
        process_valid = 0;
    } else {
        process_valid = process_matches_command(pid, expected_cmd);
    }

    if (!process_valid) {
        shm_remove_process(index);
        ipc_unlock();
        mq_send_notification(IPC_CMD_PROCESS_TERMINATED, pid);
        printf("[INFO] Process %d record cleaned (process no longer valid).\n", pid);
        return 0;
    }
    ipc_unlock();

    if (kill(pid, SIGTERM) == -1) {
        if (errno == ESRCH) {
            ipc_lock();
            index = shm_find_by_pid(pid);
            if (index >= 0) {
                shm_remove_process(index);
            }
            ipc_unlock();
            mq_send_notification(IPC_CMD_PROCESS_TERMINATED, pid);
            printf("[INFO] Process %d terminated.\n", pid);
            return 0;
        }
        perror("kill SIGTERM");
        return -2;
    }

    printf("[INFO] Signal SIGTERM sent to Process %d\n", pid);
    usleep(200000);

    if (kill(pid, 0) == 0) {
        printf("[INFO] Process %d still running, sending SIGKILL...\n", pid);
        if (kill(pid, SIGKILL) == -1 && errno != ESRCH) {
            perror("kill SIGKILL");
            return -2;
        }
        usleep(100000);
    }

    if (kill(pid, 0) == -1 && errno == ESRCH) {
        ipc_lock();
        index = shm_find_by_pid(pid);
        if (index >= 0) {
            shm_remove_process(index);
        }
        ipc_unlock();
        mq_send_notification(IPC_CMD_PROCESS_TERMINATED, pid);
        printf("[INFO] Process %d terminated successfully.\n", pid);
    } else {
        printf("[WARNING] Process %d may still be running.\n", pid);
    }
    return 0;
}

void proc_terminate_interactive(void) {
    pid_t target_pid;
    printf("Process PID to terminate: ");
    if (scanf("%d", &target_pid) != 1) {
        printf("[ERROR] Invalid PID!\n");
        clear_stdin();
        return;
    }
    clear_stdin();

    if (target_pid <= 0) {
        printf("[ERROR] PID must be positive!\n");
        return;
    }

    int result = proc_terminate(target_pid);
    if (result == -1) {
        printf("[ERROR] Process %d not found in list!\n", target_pid);
    } else if (result == -2) {
        printf("[ERROR] Failed to send signal to Process %d!\n", target_pid);
    }
}

/* Lists every active process tracked in shared memory with elapsed runtime info. */
void proc_list_all(void) {
    /* First pass: collect stale process PIDs to clean up */
    pid_t stale_pids[MAX_PROCESSES];
    int stale_count = 0;

    ipc_lock();
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (g_shared_data != NULL && g_shared_data->processes[i].is_active) {
            ProcessInfo *proc = &g_shared_data->processes[i];
            if (!proc_is_alive(proc->pid)) {
                stale_pids[stale_count++] = proc->pid;
            }
        }
    }
    ipc_unlock();

    /* Clean up stale processes outside the main listing loop */
    for (int i = 0; i < stale_count; i++) {
        ipc_lock();
        int index = shm_find_by_pid(stale_pids[i]);
        if (index >= 0) {
            printf("[CLEANUP] Removing stale process record for PID %d (no such process).\n", stale_pids[i]);
            shm_remove_process(index);
        }
        ipc_unlock();
        mq_send_notification(IPC_CMD_PROCESS_TERMINATED, stale_pids[i]);
    }

    /* Second pass: display active processes */
    ipc_lock();
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════════════════════════╗\n");
    printf("║                           RUNNING PROGRAMS                                ║\n");
    printf("╠═══════════════════════════════════════════════════════════════════════════╣\n");
    printf("║ %-7s │ %-20s │ %-10s │ %-8s │ %-8s ║\n",
           "PID", "Command", "Mode", "Owner", "Time");
    printf("╠═══════════════════════════════════════════════════════════════════════════╣\n");

    int count = 0;
    time_t now = time(NULL);
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (g_shared_data != NULL && g_shared_data->processes[i].is_active) {
            ProcessInfo *proc = &g_shared_data->processes[i];
            double elapsed = difftime(now, proc->start_time);
            char short_cmd[21];
            strncpy(short_cmd, proc->command, 20);
            short_cmd[20] = '\0';
            printf("║ %-7d │ %-20s │ %-10s │ %-8d │ %6.0fs  ║\n",
                   proc->pid,
                   short_cmd,
                   mode_str(proc->mode),
                   proc->owner_pid,
                   elapsed);
            count++;
        }
    }

    printf("╚═══════════════════════════════════════════════════════════════════════════╝\n");
    printf("Total: %d process(es)\n", count);
    ipc_unlock();
}

/* Sends SIGTERM only to attached processes owned by this instance and lets the monitor reap them. */
void proc_cleanup_attached(void) {
    pid_t my_pid = getpid();
    printf("[CLEANUP] Terminating attached processes...\n");
    pid_t targets[MAX_PROCESSES];
    int target_count = 0;

    ipc_lock();
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (g_shared_data != NULL && g_shared_data->processes[i].is_active) {
            ProcessInfo *proc = &g_shared_data->processes[i];
            if (proc->owner_pid == my_pid && proc->mode == MODE_ATTACHED) {
                printf("[CLEANUP] Terminating PID %d (%s)\n", proc->pid, proc->command);
                targets[target_count++] = proc->pid;
            }
        }
    }
    ipc_unlock();

    for (int i = 0; i < target_count; i++) {
        if (kill(targets[i], SIGTERM) == -1 && errno != ESRCH) {
            perror("kill");
        }
    }

    if (target_count > 0) {
        printf("[CLEANUP] Waiting for monitor to finish cleanup...\n");
    }
    printf("[CLEANUP] Attached process termination requests sent.\n");
}

/* Main/UI */
static void print_banner(void) {
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════════╗\n");
    printf("║                                                           ║\n");
    printf("║     ██████╗ ██████╗  ██████╗  ██████╗██╗  ██╗             ║\n");
    printf("║     ██╔══██╗██╔══██╗██╔═══██╗██╔════╝╚██╗██╔╝             ║\n");
    printf("║     ██████╔╝██████╔╝██║   ██║██║      ╚███╔╝              ║\n");
    printf("║     ██╔═══╝ ██╔══██╗██║   ██║██║      ██╔██╗              ║\n");
    printf("║     ██║     ██║  ██║╚██████╔╝╚██████╗██╔╝ ██╗             ║\n");
    printf("║     ╚═╝     ╚═╝  ╚═╝ ╚═════╝  ╚═════╝╚═╝  ╚═╝             ║\n");
    printf("║                                                           ║\n");
    printf("║     Advanced Process Management System v%s              ║\n", PROCX_VERSION);
    printf("║     Fatih Sultan Mehmet Vakif University                  ║\n");
    printf("║                                                           ║\n");
    printf("╚═══════════════════════════════════════════════════════════╝\n");
    printf("\n");
}

static void print_menu(void) {
    printf("\n");
    printf("╔════════════════════════════════════╗\n");
    printf("║         ProcX v%s                 ║\n", PROCX_VERSION);
    printf("╠════════════════════════════════════╣\n");
    printf("║  1. Run a new program              ║\n");
    printf("║  2. List running programs          ║\n");
    printf("║  3. Terminate a program            ║\n");
    printf("║  0. Exit                           ║\n");
    printf("╚════════════════════════════════════╝\n");
}

static void print_goodbye(void) {
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════════╗\n");
    printf("║  Thank you for using ProcX!                               ║\n");
    printf("║  Detached processes will continue running.                ║\n");
    printf("╚═══════════════════════════════════════════════════════════╝\n");
    printf("\n");
}

static void clear_input_buffer(void) {
    int c;
    while ((c = getchar()) != '\n' && c != EOF) {
    }
}

static int process_matches_command(pid_t pid, const char *expected_cmd) {
    char path[64];
    char cmdline[512];

    snprintf(path, sizeof(path), "/proc/%d/cmdline", pid);
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        return 0;
    }

    ssize_t len = read(fd, cmdline, sizeof(cmdline) - 1);
    close(fd);
    if (len <= 0) {
        return 0;
    }
    cmdline[len] = '\0';

    for (ssize_t i = 0; i < len; i++) {
        if (cmdline[i] == '\0') {
            cmdline[i] = ' ';
        }
    }

    char cmd_first_word[64];
    if (sscanf(expected_cmd, "%63s", cmd_first_word) != 1) {
        return 0;
    }

    return (strstr(cmdline, cmd_first_word) != NULL);
}

/**
 * Check if a process with the given PID is still alive.
 * Returns 1 if the process seems to exist, 0 if it clearly does not exist (ESRCH),
 * and 1 for other errors (treated as existing).
 */
static int proc_is_alive(pid_t pid) {
    if (pid <= 0) {
        return 0;
    }
    if (kill(pid, 0) == 0) {
        return 1;
    }
    if (errno == ESRCH) {
        return 0;
    }
    return 1;
}

static void cleanup_stale_processes_on_startup(void) {
    if (g_shared_data == NULL) {
        return;
    }

    ipc_lock();
    for (int i = 0; i < MAX_PROCESSES; i++) {
        ProcessInfo *proc = &g_shared_data->processes[i];
        if (!proc->is_active) {
            continue;
        }

        if (kill(proc->owner_pid, 0) == -1 && errno == ESRCH) {
            printf("[STARTUP CLEANUP] Removing orphaned process record: PID %d (owner %d no longer exists)\n",
                   proc->pid, proc->owner_pid);
            proc->is_active = 0;
            proc->status = STATUS_TERMINATED;
            if (g_shared_data->process_count > 0) {
                g_shared_data->process_count--;
            }
            continue;
        }

        if (kill(proc->pid, 0) == -1 && errno == ESRCH) {
            printf("[STARTUP CLEANUP] Removing dead process record: PID %d\n", proc->pid);
            proc->is_active = 0;
            proc->status = STATUS_TERMINATED;
            if (g_shared_data->process_count > 0) {
                g_shared_data->process_count--;
            }
        }
    }
    ipc_unlock();
}

/* Initializes signal handlers, IPC primitives, and worker threads before entering the UI loop. */
static int initialize_program(void) {
    printf("[INIT] Starting ProcX (PID: %d)...\n", getpid());
    if (setup_signal_handlers() != 0) {
        fprintf(stderr, "[FATAL] Failed to setup signal handlers!\n");
        return -1;
    }
    if (ipc_init_all() != 0) {
        fprintf(stderr, "[FATAL] Failed to initialize IPC resources!\n");
        return -1;
    }
    cleanup_stale_processes_on_startup();
    if (threads_start_all() != 0) {
        fprintf(stderr, "[FATAL] Failed to start threads!\n");
        ipc_cleanup_all(0);
        return -1;
    }
    printf("[INIT] ProcX is ready!\n");
    return 0;
}

/* Coordinates graceful shutdown: terminate attached children, run a final scan, stop threads, release IPC. */
static void shutdown_program(void) {
    printf("\n[SHUTDOWN] Shutting down ProcX...\n");
    printf("[SHUTDOWN] Cleaning up attached processes...\n");
    proc_cleanup_attached();
    printf("[SHUTDOWN] Forcing one last monitor scan...\n");
    monitor_scan_once(getpid());
    if (threads_are_running()) {
        sleep(1);
    }
    printf("[SHUTDOWN] Stopping threads...\n");
    threads_stop_all();
    printf("[SHUTDOWN] Releasing IPC resources...\n");
    ipc_cleanup_all(0);
    printf("[SHUTDOWN] Cleanup complete.\n");
}

static void handle_menu_choice(int choice) {
    switch (choice) {
        case 1:
            proc_start_interactive();
            break;
        case 2:
            proc_list_all();
            break;
        case 3:
            proc_terminate_interactive();
            break;
        case 0:
            break;
        default:
            printf("[ERROR] Invalid choice! Please enter 0-3.\n");
            break;
    }
}

static void main_menu_loop(void) {
    int choice;
    while (is_running()) {
        print_menu();
        printf("Your choice: ");
        if (scanf("%d", &choice) != 1) {
            printf("[ERROR] Invalid input! Please enter a number.\n");
            clear_input_buffer();
            continue;
        }
        clear_input_buffer();
        if (choice == 0) {
            printf("\n[INFO] Exit requested by user.\n");
            request_shutdown();
            break;
        }
        handle_menu_choice(choice);
    }
}

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;

    print_banner();
    if (initialize_program() != 0) {
        fprintf(stderr, "\n[FATAL] Failed to initialize ProcX. Exiting.\n");
        return EXIT_FAILURE;
    }
    main_menu_loop();
    shutdown_program();
    print_goodbye();
    return EXIT_SUCCESS;
}
