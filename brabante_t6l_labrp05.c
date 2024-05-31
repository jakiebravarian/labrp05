// Author: Jakie Ashley C. Brabante
// Section: T-6L
// Lab Problem: 5
// Description: Distributed Computation of the Pearson Correlation Coefficient

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
#include <math.h>

#define IP_LENGTH 16
#define KB 1024

//////////////////////////////////////////////////////////////////////////////////////////////////

typedef struct {
    char *ip;
    int port;
} address;

typedef struct {
    double **transposed_matrix;
    double *vector_y;
    double *results;
    int n;
    int start_index; 
    int end_index;
    
    int t_number;
    address *slave_address;
} master_args_t;

typedef struct {
    int sockfd;  
    int connfd;  
    socklen_t addr_len;      
} SocketConnection;

typedef struct {
    double **matrix;
    double *vector_y;
    double *results;
    int n;
    int start_index; 
    int end_index;
} pearson_args_t;


//////////////////////////////////////////////////////////////////////////////////////////////////

// function declarations
void master(int n, int p, int t, address **slave_addresses);
void* master_t(void *args);
void slave(int n, int p, int t, address *master_address, address *slave_address);


void setThreadCoreAffinity(int thread_number);
SocketConnection* connectToServer(const char* ip, int port);
SocketConnection* initializeServerSocket(const char* ip, int port);

void sendData(double **matrix, double *vector_y, int n, int start_index, int end_index, int sockfd);
pearson_args_t* receiveData(SocketConnection *conn, pearson_args_t *data);

void pearson_cor(pearson_args_t* args);
void sendResult(int connfd, pearson_args_t* data);
void receiveResult(int sockfd, double *results, int n, int start_index, int end_index);

void handleError(const char* message);
double get_elapsed_time(struct timespec start, struct timespec end);
void printMatrix(char *matrix_name, double **matrix, int row, int col);
void printVector(char *vector_name, double *vector, int size);

///////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char *argv[]) {
    
    //  Read input from command line --------------------------------------------------------------
    int n = atoi(argv[1]);      //  n size of matrix
    int p = atoi(argv[2]);      //  port number
    int s = atoi(argv[3]);      //  status (0 = master | 1 = slave)
    int t = atoi(argv[4]);      //  number of threads


    //  Read IP Addresses and Port Numbers in Config File -----------------------------------------
    char filename[255];
    sprintf(filename, "config_%d.cfg", t);
    printf("READING FROM CONFIG FILE: %s\n", filename);

    FILE *file = fopen(filename, "r");
    if (!file) {
        handleError("Failed to open file");
    }

    //  Get master ip and port
    address *master_address = malloc(sizeof(address)); ;
    master_address->ip = malloc(sizeof(char) * IP_LENGTH);
    fscanf(file, "\n%[^:]:%d", master_address->ip, &master_address->port); 
    
    //  Get slaves ip and port
    int num_slaves = 0;
    fscanf(file, "%d", &num_slaves);
    printf("NUMBER OF SLAVES: %d\n", num_slaves);

    address **slave_addresses = malloc(sizeof(address) * num_slaves);
    for(int i = 0; i <  num_slaves; i++){
        address *slave_address = malloc(sizeof(address));
        slave_address->ip = malloc(sizeof(char) * IP_LENGTH);

        fscanf(file, "\n%[^:]:%d", slave_address->ip, &slave_address->port);
        printf("Slave %d  -  %s  -  %d\n", i, slave_address->ip, slave_address->port);

        slave_addresses[i] = slave_address;
    }

        printf("===========================================================================\n\n");

    //  Run master process ------------------------------------------------------------------------
    if (s == 0) {
        master(n, p, t, slave_addresses);
        return 0;
    }

    //  Run slave process -------------------------------------------------------------------------
    int index = (p - slave_addresses[0]->port) % t;
    slave(n, p, t, master_address, slave_addresses[index]);

    fclose(file);
}

void master(int n, int p, int t, address **slave_addresses) {

    //  Indicate that the master is now running ---------------------------------------------------
    printf("MASTER is now LISTENING at PORT %d", p);
    printf("\n\n===========================================================================\n\n");


    //  Create matrix and vector y ----------------------------------------------------------------
    double** input_matrix = malloc(n * sizeof(double*));
    for (int i = 0; i < n; i++) input_matrix[i] = malloc(n * sizeof(double));

    double* vector_y = malloc(n * sizeof(double));

    //  Populate matrix ---------------------------------------------------------------------------

    //  If n = 10, populate with dummy data from exer 1
    if (n == 10) {
        printf("Inputted matrix size is 10x10. Will now POPULATE WITH DUMMY DATA. \n");
        for(int i = 0; i < 10; i++){
            input_matrix[0][i] = 3.63;
            input_matrix[1][i] = 3.02;
            input_matrix[2][i] = 3.82;
            input_matrix[3][i] = 3.42;
            input_matrix[4][i] = 3.59;
            input_matrix[5][i] = 2.87;
            input_matrix[6][i] = 3.03;
            input_matrix[7][i] = 3.46;
            input_matrix[8][i] = 3.36;
            input_matrix[9][i] = 3.3;
        }

        vector_y[0] = 53.1;
        vector_y[1] = 49.7;
        vector_y[2] = 48.4;
        vector_y[3] = 54.2;
        vector_y[4] = 54.9;
        vector_y[5] = 43.7;
        vector_y[6] = 47.2;
        vector_y[7] = 45.2;
        vector_y[8] = 54.4;
        vector_y[9] = 50.4;  

        printMatrix("Input Matrix", input_matrix, n, n);
        printVector("y", vector_y, n);

        printf("\n===========================================================================\n\n");
        
    }

    //  Else, populate with random non-zero integers
    else {
        srand((unsigned int)time(NULL));
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                input_matrix[i][j] = (double)(rand() % 100 + 1);
            }
            vector_y[i] = (double)(rand() % 100 + 1);
        }
    }
    

    //  Transpose matrix --------------------------------------------------------------------------
    double** transposed_matrix = malloc(n * sizeof(double*));
    for (int i = 0; i < n; i++) transposed_matrix[i] = malloc(n * sizeof(double));
    for(int i = 0; i < n; i++)
        for(int j = 0; j < n; j++)
            transposed_matrix[j][i] = input_matrix[i][j];

    if (n <= 10) {
        printMatrix("Transposed Matrix", transposed_matrix, n, n);
        printf("\n\n===========================================================================\n\n");
    }


    //  Divide columns to t threads ---------------------------------------------------------------
    int work_per_thread = n / t;
    int remaining_work = n % t;
    int* starting_index_list = malloc(t * sizeof(int));
    int* ending_index_list = malloc(t * sizeof(int));
    
    for(int i = 0; i < t; i++){
        int start_index = i * work_per_thread + (i < remaining_work ? i : remaining_work);
        starting_index_list[i] = start_index;

        int end_index = start_index + work_per_thread + (i < remaining_work ? 1 : 0);
        ending_index_list[i] = end_index;
    }


    //  Distribute matrix to slaves ---------------------------------------------------------------
    pthread_t *threads = malloc(t * sizeof(pthread_t));
    master_args_t *args = malloc(t * sizeof(master_args_t));
    double** thread_results = malloc(t * sizeof(double*)); // Allocate array of pointers for each thread's result

    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    for (int i = 0; i < t; i++) {
        args[i] = (master_args_t){.transposed_matrix = transposed_matrix, .vector_y = vector_y, .n = n, .start_index = starting_index_list[i], .end_index = ending_index_list[i], .t_number = i, .slave_address = slave_addresses[i]};
        args[i].results = malloc((ending_index_list[i] - starting_index_list[i]) * sizeof(double));  // Allocate individual results array
        thread_results[i] = args[i].results; // Store pointer for aggregation
        
        pthread_create(&threads[i], NULL, master_t, (void *)&args[i]);
    }


    //  Join threads ------------------------------------------------------------------------------
    for (int i = 0; i < t; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &end);
    double time_elapsed = get_elapsed_time(start, end);
    printf("Time Elapsed: %f seconds\n", time_elapsed);

    // Aggregate results from each thread
    double *final_results = malloc(n * sizeof(double));
    for (int i = 0; i < t; i++) {
        for (int j = 0; j < (args[i].end_index - args[i].start_index); j++) {
            final_results[args[i].start_index + j] = thread_results[i][j];
        }
    }

    if(n <= 10){
        printVector("Collated vector r", final_results, n);
    }


    //  Free allocated memory ---------------------------------------------------------------------
    free(final_results);
    for (int i = 0; i < t; i++) free(args[i].results);
    free(args);
    free(threads);
    free(starting_index_list);
    for (int i = 0; i < n; i++) free(transposed_matrix[i]);
    free(transposed_matrix);
    free(vector_y);
    for (int i = 0; i < n; i++) free(input_matrix[i]);
    free(input_matrix);

}

void* master_t(void *args) {

    //  Get args ----------------------------------------------------------------------------------
    master_args_t* actual_args = (master_args_t*)args;

    double **matrix = actual_args->transposed_matrix;
    double *vector_y = actual_args->vector_y;
    double *results = actual_args->results;
    int n = actual_args->n;
    int start_index = actual_args->start_index;
    int end_index = actual_args->end_index;
    int t_number = actual_args->t_number;
    address *slave_address = actual_args->slave_address;


    //  Set core affinity of thread ---------------------------------------------------------------
    setThreadCoreAffinity(t_number);


    //  Create a socket for the thread ------------------------------------------------------------
    SocketConnection *conn = connectToServer(slave_address->ip, slave_address->port);
    printf("Successfully connected to %s:%d with sockfd = %d\n", slave_address->ip, slave_address->port, conn->sockfd);

    //  Send data ---------------------------------------------------------------------------------
    printf("[%d] Master sending %d (rows) by %d (cols)\n", t_number, (end_index - start_index), n);
    sendData(matrix, vector_y, n, start_index, end_index, conn->sockfd);

    //  Receive result ----------------------------------------------------------------------------
    receiveResult(conn->sockfd, results, n, start_index, end_index);

    close(conn->connfd);
    close(conn->sockfd);
    free(conn);
    return NULL;
}

void slave(int n, int p, int t, address *master_address, address *slave_address) {

    //  Set core affinity of slave ----------------------------------------------------------------
    setThreadCoreAffinity(slave_address->port);


    //  Create socket for slave -------------------------------------------------------------------
    printf("\nSTARTING SLAVE %s:%d\n", slave_address->ip, slave_address->port);
    SocketConnection *conn = malloc(sizeof(SocketConnection));
    conn = initializeServerSocket(slave_address->ip, slave_address->port);
    
    if(conn->sockfd == -1) {
        handleError("Error: Server socket initialization failed.\n");
    }
    
    printf("\nSlave listening at port %d\n", slave_address->port);


    //  Receive data from master ------------------------------------------------------------------
    pearson_args_t *data = malloc(sizeof(pearson_args_t));
    receiveData(conn, data);

    //  Compute for r -----------------------------------------------------------------------------
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    printf("Start computation of pearson\n");
    pearson_cor(data);
    printf("End computation of pearson\n");

    clock_gettime(CLOCK_MONOTONIC, &end);
    double time_elapsed = get_elapsed_time(start, end);
    printf("(Pearson Computation) Time Elapsed: %f seconds\n", time_elapsed);

    //  Send vector r to master -------------------------------------------------------------------
    sendResult(conn->connfd, data);

    
    // Free data and close connection -------------------------------------------------------------
    if (data->matrix) {
        for (int i = 0; i < (data->end_index - data->start_index); i++) {
            free(data->matrix[i]);
        }
        free(data->matrix);
    }
    if (data->vector_y) free(data->vector_y);
    free(data);

    close(conn->connfd);
    close(conn->sockfd);
    free(conn);
}

void pearson_cor(pearson_args_t* args) {
    args->results = malloc(sizeof(double) * args->n);
    int length = args->end_index - args->start_index;
    for (int i = 0; i < length; i++) {
        double sum_x = 0, sum_y = 0, sum_xx = 0, sum_yy = 0, sum_xy = 0;
        for (int j = 0; j < args->n; j++) {
            double x = args->matrix[i][j];
            double y = args->vector_y[j];
            sum_x  += x;
            sum_xx += x * x;
            sum_y += y;
            sum_yy += y * y;
            sum_xy += x * y;
        }
        double numerator = args->n * sum_xy - sum_x * sum_y;
        double denominator = sqrt(fabs((args->n * sum_xx - sum_x * sum_x) * (args->n * sum_yy - sum_y * sum_y)));
    
        double result = (denominator == 0 || isnan(numerator) || isnan(denominator)) ? 0 : numerator / denominator;
        args->results[i] = result;
    }
}

void setThreadCoreAffinity(int thread_number) {
    // Retrieve the number of online processors and calculate the count of physical cores
    int total_cores = sysconf(_SC_NPROCESSORS_ONLN);
    int physical_cores = total_cores / 2;

    // Initialize CPU set to manage processor affinity
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);

    // Calculate an appropriate CPU assignment for the thread
    // Aim to distribute threads across available physical cores
    int base_core = thread_number % (physical_cores - 1);
    int toggle_even_odd = (thread_number / (physical_cores - 1)) % 2;
    int cpu_to_assign = (base_core + 1) * 2 + toggle_even_odd;

    // Set the thread to run on the calculated CPU, skipping CPU 0
    CPU_SET(cpu_to_assign, &cpu_set);

    // Apply the CPU set to the current thread
    pthread_t this_thread = pthread_self();
    if (pthread_setaffinity_np(this_thread, sizeof(cpu_set_t), &cpu_set) != 0) {
        handleError("Failed to set thread core affinity");
    }
}

SocketConnection* connectToServer(const char* ip, int port) {
    SocketConnection* conn = malloc(sizeof(SocketConnection));
    if (!conn) {
        handleError("Failed to allocate memory for SocketConnection");
        return NULL;
    }

    // Create socket
    conn->sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (conn->sockfd == -1) {
        handleError("Socket creation failed");
        free(conn);
        return NULL;
    }

    // Define the server address structure
    struct sockaddr_in server_addr = {0};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
        handleError("Invalid address / Address not supported");
        close(conn->sockfd);
        free(conn);
        return NULL;
    }

    // Connect to the server
    if (connect(conn->sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) != 0) {
        handleError("Connection Failed");
        close(conn->sockfd);
        free(conn);
        return NULL;
    }

    return conn;
}

SocketConnection* initializeServerSocket(const char* ip, int port) {
    SocketConnection* conn = malloc(sizeof(SocketConnection));
    if (!conn) {
        handleError("Failed to allocate memory for SocketConnection");
        return NULL;
    }

    // Create socket
    conn->sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (conn->sockfd == -1) {
        handleError("Socket creation failed");
        free(conn);
        return NULL;
    } else {
        printf("Slave's socket successfully created.\n");
    }
    

    struct sockaddr_in client_addr;
    struct sockaddr_in server_addr = {0};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip);
    server_addr.sin_port = htons(port);

    // Bind socket
    if (bind(conn->sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) != 0) {
        handleError("Failed to bind socket");
        close(conn->sockfd);
        free(conn);
        return NULL;
    } else {
        printf("Slave's socket successfully bound.\n");
    }
    
    // Listen on socket
    if (listen(conn->sockfd, 5) != 0) {
        handleError("Failed to listen on socket");
        close(conn->sockfd);
        free(conn);
        return NULL;
    } else {
        printf("Server socket is now listening for connections...\n");
    }
    

    conn->addr_len = sizeof(client_addr);
    conn->connfd = accept(conn->sockfd, (struct sockaddr*)&client_addr, &conn->addr_len);
    if (conn->connfd < 0) {
        handleError("Failed to accept connection");
        close(conn->sockfd);
        free(conn);
        return NULL;
    } else {
        printf("Connection accepted.\n");
    }
    

    return conn;
}

void sendData(double **matrix, double *vector_y, int n, int start_index, int end_index, int sockfd) {
    
    //  Send matrix info --------------------------------------------------------------------------
    int matrix_info[] = {start_index, end_index, n};
    write(sockfd, matrix_info, sizeof(matrix_info));

    //  Send matrix data --------------------------------------------------------------------------
    unsigned int element_size = sizeof(double);
    int num_elements = KB / element_size;
    int groups_per_row = n / num_elements;
    int remaining_elements = n % num_elements;

    for(int i = start_index; i < end_index; i++){
        for(int j = 0; j < groups_per_row ; j++){
            int start_index = j * num_elements;
            write(sockfd, &matrix[i][start_index], num_elements * element_size);
        }
        write(sockfd, &matrix[i][groups_per_row  * num_elements], remaining_elements * element_size);
    }

    //  Send vector data --------------------------------------------------------------------------
    for (int j = 0; j < groups_per_row; j++) {
        int start_index = j * groups_per_row;
        write(sockfd, &vector_y[start_index], num_elements * element_size);
    }
    write(sockfd, &vector_y[groups_per_row * num_elements], remaining_elements * element_size);
    
    return;

}

pearson_args_t* receiveData(SocketConnection* conn, pearson_args_t *data) {
    int connfd = conn->connfd;  

    //  Get matrix info ---------------------------------------------------------------------------
    int matrix_info[3];
    read(connfd, matrix_info, sizeof(matrix_info));
    int rows_to_receive = matrix_info[1] - matrix_info[0];
    data->n = matrix_info[2];
    data->start_index = matrix_info[0]; 
    data->end_index = matrix_info[1];

    // Allocate memory for matrix and vector
    data->matrix = malloc(sizeof(double*) * rows_to_receive);
    if (data->matrix == NULL) {
        perror("Failed to allocate memory for matrix rows");
        return NULL;
    }

    data->vector_y = malloc(sizeof(double) * data->n);
    if (data->vector_y == NULL) {
        perror("Failed to allocate memory for vector y");
        free(data->matrix);
        return NULL;
    }


    //  Get matrix data ---------------------------------------------------------------------------
    unsigned int element_size = sizeof(double);
    int num_elements = KB / element_size;
    int groups_per_row = data->n / num_elements;
    int remaining_elements = data->n % num_elements;

    for (int i = 0; i < rows_to_receive; i++) {
        data->matrix[i] = malloc(sizeof(double) * data->n);
        for (int j = 0; j < groups_per_row; j++) {
            int start_index = j * num_elements;
            read(connfd, &(data->matrix[i][start_index]), num_elements * element_size);
        }
        read(connfd, &(data->matrix[i][groups_per_row * num_elements]), remaining_elements * element_size);
    }

    //  Get vector data ---------------------------------------------------------------------------
    for (int j = 0; j < groups_per_row; j++) {
        int start_index = j * num_elements;
        read(connfd, &(data->vector_y[start_index]), num_elements * element_size);
    }
    read(connfd, &(data->vector_y[groups_per_row * num_elements]), remaining_elements * element_size);

    printf("Received %d (rows) by %d (cols) matrix, and vector of length %d\n", rows_to_receive, data->n, data->n);
    
    if (data->n <= 10) printMatrix("Received Matrix", data->matrix, rows_to_receive, data->n);
    if (data->n <= 10) printVector("Received Vector y", data->vector_y, data->n);

    return data;
}

void sendResult(int connfd, pearson_args_t* data) {

    //  Send vector r data --------------------------------------------------------------------------
    int length = data->end_index - data->start_index;
    if (data->n <= 10) printVector("Vector r to be sent", data->results, length);

    unsigned int element_size = sizeof(double);
    int num_elements = KB / element_size;
    int groups_per_row = length / num_elements;
    int remaining_elements = length % num_elements;

    for (int i = 0; i < groups_per_row; i++) {
        int start_index = i * num_elements;
        send(connfd, &data->results[start_index], num_elements * element_size, 0);
    }
    send(connfd, &data->results[groups_per_row * num_elements], remaining_elements * element_size, 0);

    return;

}

void receiveResult(int sockfd, double *results, int n, int start_index, int end_index) {

    //  Receive vector r data -----------------------------------------------------------------------
    int length = end_index - start_index;
    unsigned int element_size = sizeof(double);
    int num_elements = KB / element_size;
    int groups_per_row = length / num_elements;
    int remaining_elements = length % num_elements;

    for(int j = 0; j < groups_per_row; j++){
        int start_index = j * num_elements;
        recv(sockfd, &(results[start_index]), num_elements * element_size, 0);
    }
    recv(sockfd, &(results[groups_per_row * num_elements]), remaining_elements * element_size, 0);

    if (n <= 10) printVector("Received vector r", results, length);

    return;
}

void handleError(const char* message) {
    perror(message);
    exit(EXIT_FAILURE);
}

double get_elapsed_time(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

void printMatrix(char *matrix_name, double **matrix, int row, int col) {
    printf("\n%s:\n", matrix_name);
    for (int i = 0; i < row; i++) {
        for (int j = 0; j < col; j++) {
            printf("%.2f\t", matrix[i][j]);
        }
        printf("\n");
    }
}

void printVector(char *vector_name, double *vector, int size) {
    printf("\n%s:\n", vector_name);
    for (int i = 0; i < size; i++) {
        printf("%.2f\t", vector[i]);
    }
    printf("\n");
}
