// Author: Jakie Ashley C. Brabante
// Section: T-6L
// Lab Problem: 4
// Description: Distributing Parts of a Matrix over Sockets

//////////////////////////////////////////////////////////////////////////////////////////////////

#define _GNU_SOURCE 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>
#include <sched.h>
#include <stdbool.h>
#include <errno.h> 
#include <pthread.h>

#define NUM_CORES 3

// hods the ip address and port number of master/slave AND core index for core-affinity
typedef struct {
    char ip[16];
    int port;
    int index;
} Address;

// function declarations
Address readConfig(char *filename, int *num_slaves, Address **slave_addresses);
void master(int matrix_size, int port_number, Address *slaves, int num_slaves);
void slave(int matrix_size, int port);

double get_elapsed_time(struct timespec start, struct timespec end);

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s matrix_size port status(0=master|1=slave)\n", argv[0]);
        return EXIT_FAILURE;
    }
    
    //  Read input from command line
    int n = atoi(argv[1]);      //  n size of matrix
    int p = atoi(argv[2]);      //  port number
    int s = atoi(argv[3]);      //  status (0 = master | 1 = slave)

    int num_slaves;
    Address *slave_addresses = NULL;
    readConfig("config.cfg", &num_slaves, &slave_addresses);

    //  If s = 0 (i.e. master)
    if (s == 0) {
        master(n, p, slave_addresses, num_slaves);

    //  If s = 1 (i.e. slave)
    } else if (s == 1) {
        slave(n, p);

    } else {
        fprintf(stderr, "Wrong input. Use 0 for master, 1 for slave.\n");
        return EXIT_FAILURE;
    }

    free(slave_addresses);
    return 0;
}

Address readConfig(char *filename, int *num_slaves, Address **slave_addresses) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }

    Address master;
    fscanf(file, "%[^:]:%d", master.ip, &master.port);  // Read master address
    fscanf(file, "%d", num_slaves); // Read number of slaves

    *slave_addresses = malloc(sizeof(Address) * (*num_slaves));
    for (int i = 0; i < *num_slaves; i++) {
        fscanf(file, "%[^:]:%d %d", (*slave_addresses)[i].ip, &(*slave_addresses)[i].port, &(*slave_addresses)[i].index);
    }

    fclose(file);
    return master;
}

int get_current_cpu() {
    return sched_getcpu();
}

void master(int matrix_size, int port_number, Address *slaves, int num_slaves) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(2, &cpuset);
    pthread_t current_thread = pthread_self();
    if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
        perror("Error setting affinity for master");
        exit(EXIT_FAILURE);
    }
    printf("Master set to CPU %d\n", 2);

    printf("Running on CPU: %d\n", get_current_cpu());


    int** matrix = (int**)malloc(matrix_size * sizeof(int*));
    for (int i = 0; i < matrix_size; i++) {
        matrix[i] = (int*)malloc(matrix_size * sizeof(int));
        for (int j = 0; j < matrix_size; j++) {
            matrix[i][j] = rand() % 100 + 1; // Assign random non-zero integers
        }
    }

    int rows_per_slave = matrix_size / num_slaves;

    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    int ack_count = 0;  // Counter for acknowledgments

    // Loop through the array of slaves to initiate communication
    for (int i = 0; i < num_slaves; i++) {

        // Create a new socket for each slave
        int sock = socket(AF_INET, SOCK_STREAM, 0);

        if (sock < 0) {
            perror("Socket creation failed");
            continue;
        }

        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(slaves[i].port);
        inet_pton(AF_INET, slaves[i].ip, &serv_addr.sin_addr);


        // Set socket timeout options
        struct timeval timeout;
        timeout.tv_sec = 5;  // Timeout after 5 seconds
        timeout.tv_usec = 0;

        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof timeout);
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof timeout);


        // Attempt to connect to the current slave
        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            printf("Connection Failed with Slave %d\n", i+1);
            continue;
        }

        // Determine submatrix to send
        int start_row = i * rows_per_slave;
        int end_row = (i == num_slaves - 1) ? matrix_size : (i + 1) * rows_per_slave;
        int num_rows = end_row - start_row;

        // Send the number of rows first
        // printf("Sending %d rows to slave %d...\n", num_rows, i + 1);    // PRINT DEBUG
        send(sock, &num_rows, sizeof(int), 0);

        // Serialize submatrix
        for (int row = start_row; row < end_row; row++) {
            send(sock, matrix[row], matrix_size * sizeof(int), 0);
        }

        // // Serialize submatrix and send it || PRINT DEBUG
        // for (int row = start_row; row < end_row; row++) {
        //     printf("Sending row %d to slave %d: ", row, i + 1);
        //     for (int col = 0; col < matrix_size; col++) {
        //         printf("%d ", matrix[row][col]);
        //     }
        //     printf("\n");
        //     send(sock, matrix[row], matrix_size * sizeof(int), 0);
        // }

        // Wait for and read the response from the slave
        char buffer[1024] = {0};
        if (read(sock, buffer, 1024) > 0) {
            printf("Message received from slave %d: %s\n", i+1, buffer);
            ack_count++;
        } else {
            printf("Failed to receive message from slave %d\n", i+1);
        }

        close(sock);  // Close the socket after communication
    }

    if (ack_count != num_slaves) {
        printf("Not all slaves responded correctly. Expected %d ACKs, received %d ACKs.\n", num_slaves, ack_count);
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    double time_elapsed = get_elapsed_time(start, end);
    printf("Time Elapsed: %f seconds\n", time_elapsed);

    // Free matrix
    for (int i = 0; i < matrix_size; i++) free(matrix[i]);
    free(matrix);
}

void slave(int matrix_size, int port_number) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0); // Create a socket file descriptor for the server
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_addr.s_addr = INADDR_ANY,  // Listen on all available interfaces
        .sin_port = htons(port_number), // Convert the port number to network byte order
    };

    // Bind the socket to the specified port
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for incoming connections
    if (listen(server_fd, 3) < 0) { // Can handle up to 3 queued connections
        perror("listen");
        exit(EXIT_FAILURE);
    }

    int addrlen = sizeof(address);
    int new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
    if (new_socket < 0) {
        perror("accept");
        exit(EXIT_FAILURE);
    }

    // Read the configuration to find the current process's core index
    Address myConfig;
    FILE *file = fopen("config.cfg", "r");
    if (!file) {
        perror("Failed to open config file");
        exit(EXIT_FAILURE);
    }

    char line[256];
    bool configFound = false;
    while (fgets(line, sizeof(line), file)) {
        Address addr;
        if (sscanf(line, "%15[^:]:%d %d", addr.ip, &addr.port, &addr.index) == 3) {
            if (addr.port == port_number) {
                myConfig = addr;
                configFound = true;
                break;
            }
        }
    }
    fclose(file);

    if (!configFound) {
        fprintf(stderr, "Configuration for port %d not found.\n", port_number);
        exit(EXIT_FAILURE);
    }

    // Set CPU affinity for the current slave process
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(myConfig.index % NUM_CORES, &cpuset);
    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) < 0) {
        perror("sched_setaffinity failed");
        exit(EXIT_FAILURE);
    }

    printf("Slave running on port %d set to CPU %d\n", port_number, myConfig.index % NUM_CORES);

    printf("Running on CPU: %d\n", get_current_cpu());

    // Read the number of submatrix rows
    int submatrix_rows;
    recv(new_socket, &submatrix_rows, sizeof(int), 0);

    // PRINT DEBUG
    // printf("Received %d rows from master.\n", submatrix_rows); 

    // Allocate and receive the submatrix
    int** submatrix = (int**)malloc(submatrix_rows * sizeof(int*));
    for (int i = 0; i < submatrix_rows; i++) {
        submatrix[i] = (int*)malloc(matrix_size * sizeof(int));
        recv(new_socket, submatrix[i], matrix_size * sizeof(int), 0);
    }

    // // Allocate space for submatrix | PRINT DEBUG
    // int** submatrix = (int**)malloc(submatrix_rows * sizeof(int*));
    // for (int i = 0; i < submatrix_rows; i++) {
    //     submatrix[i] = (int*)malloc(matrix_size * sizeof(int));
    //     recv(new_socket, submatrix[i], matrix_size * sizeof(int), 0);
    //     printf("Received row %d from master: ", i);
    //     for (int j = 0; j < matrix_size; j++) {
    //         printf("%d ", submatrix[i][j]);
    //     }
    //     printf("\n");
    // }

    // Send acknowledgment
    char* response = "ack";
    send(new_socket, response, strlen(response), 0);

    // Free allocated memory
    for (int i = 0; i < submatrix_rows; i++) free(submatrix[i]);
    free(submatrix);

    close(new_socket); // Close the client socket
    close(server_fd);  // Close the listening socket
}

double get_elapsed_time(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}