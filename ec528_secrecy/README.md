## Building a Secure Communication Layer for Multi-Party Computing - Project Description

## 1. Visions and Goals of the Project

Our communication layer will replace Message Passing Interface (MPI), the messaging protocol currently used in Secrecy:

  - Eliminate MPI dependency in Secrecy and establish standing TCP connections
  - Run our Secrecy prototype on a Linux Unikernel (UKL)

MPI is very effective for High Performance Computing on a single cluster. However, since MPC parties are not necessarily located in the same place, a communication protocol that uses the internet (i.e. TCP) is needed. Additionally, Secrecy is meant to eventually be deployed on a Unikernel, which MPI is not compatible with.

## 2. Users and Personas of the Project

In order for outside parties to benefit from MPC, developers will implementing this improved software. They will benefit from a faster communication layer that enables MPC computation of a data owner's sensitive data at an improved rate.

Developers of the MPC software with this faster communication layer will be the main users of this project as implementation on a unikernel rather than reliance of MPI communication will allow for faster computation.

This project does not target those outside parties (data owner and data learner) that are inputting and visualizing this sensitive data. They will not be interacting with the communication layer as that will be the task of the developers. They will, however, also benefit from faster computation speeds regarding their sensitive data. 

## 3. Scope and Features of the Project

- Remove dependencies on MPI
    - Replace MPI init with a TCP init function
       - Calls on TCPConnect and TCPAccept functions where sockets between parties are established
    - Establish TCP connections between parties involved and orchestration mechanism
    - Asynchronous Communication
       - TCP is intrinsically asynchronous
    - Maintain proper function of all other aspects of current Secrecy framework
       - Utilized binary equality and group by join experiments to verify proper implementation 

- Orchestration
    - Implement way to orchestrate party IP addresses for TCP
    - Master Orchestrator: spawns processes/waits for parties to contact it to pass IP addresses to other parties
    - Creates a text file that is passed to all parties that are used in TCP_Init

- TCP
    - Network communication protocol that uses IP addresses and port number for routing
    - It rearranges data packets in the order specified with guarantee that they will be received in the same order sent
    - Does flow control
    - Programmed using C sys/socket library
    - To set up standard socket connection, three data network packets to set up the socket connection
    
    ![image](https://github.com/msisk23/MPC_Project/blob/main/TCP%20Flow%20Diagram.png)
    
    _**Figure 1: Flow Diagram of how a TCP Server operates.**_ 

- Unikernel deployment
    - Benchmarking of MPI alternative
    - Ability to run communication-intensive applications using our MPI alternative on a Linux Unikernel (UKL)
    - Report performance improvements when using UKL (if any)

- Unikernel
    - With help from other UKL teams, we currently have one instance of Secrecy running on the Unikernel as a proof of concept. Without personal knowledge of how to run programs on the UKL, the addition and connection of all three parties of Secrecy will need to be carried out by those more familiar with UKL.

## 4. Solution Concept
**Global Architectural Structure of the Project**

Crucial project components and definitions:
  - MPI: Message Passing Interface - commonly used in high performance computing, is very effective and efficient on a single cluster. Doesn't necessarily work with MPC due to physical locations of different parties, and is incompatible with UKL
  - Party: One of three web services used during the data transfer process. The "hub" where messages are sent or received.
  - Web: Cloud providers that provide machines were secure computations on supplied data are taking place.
  - Multi Party Communication (MPC): Communication between three cloud services to ensure secure data transmission and evaluation
  - Secrecy: Application used to execute secure relational analytics according to a cryptographic MPC protocol
  - Master Orchestrator: entity that receives IP addresses of parties and passes them to other parties to establish socket connections and open TCP flow
![image](https://user-images.githubusercontent.com/61120367/134678604-cf5f5657-4c49-4310-be77-839b6323eb1e.png)

_**Figure 2: Architecture of the MPC. Black components currently in use, blue components to be implemented.**_

Figure 2 demonstrates the current structure of the MPC, and the structure to be implemented. MPI was used to enable communication between parties. During certain points of program execution, parties have to verify information with each other. In the current MPC implementation, parties can only communicate along the Main Thread one message at a time. In order to establish a non-blocking method of asynchronous verification, a Communication Thread (seen in blue) will replace MPI. Using TCP communication, input and output buffers will allow for the non-blocking transfer of data. Later, the data will be processed asynchronously to provide verification for each party. 

**Design Implications and Discussion**

Key Design Decisions and Implementations:
  - MPI Elimination: MPI was first deployed as a temporary solution. In order to deploy Secrecy for MPC, remove unnecessary software depenencies and run on UKL, MPI needs to be replaced.
      - This will be done by implementing TCP connections between parties
  - MOC Testing of TCP Implementation: Using the Mass Open Cloud, we were able to simulate three separate parties that would be used in Secrecy. With limited prior knowledge on the MOC and how to properly create instances, there were some modifications that we needed to make to allow our TCP version to run properly. We had arbitarily decided that port 8000 would be used when initializing the sockets in our send and receive functions, however, we did not realize that this port was not configured on MOC instances. By properly setting security groups within each VM to allow for connections on Port 8000, we were able to move past this roadblock and begin testing on the MOC.
  - Persistent Initialization of Sockets: Rather than creating a new socket and connecting and accepting to that new initialization for each message, we initialized a socket between each party when Secrecy is started. From there, we are able to use those sockets to send and receive messages throughout the entirety of an experiment. This reduces overhead and allows for faster communication between each party involved in the computation.
  - Unikernel Implementation: After verifying functionality of the MPI-free system, MPC will run on top of a Unikernel. The stripped down implementation will further speed up MPC implementation. 


## 5. Acceptance Criteria

Minimum acceptance is defined as replacing MPI in Secrecy with functioning TCP connections and implementing a party orchestrator so that our solution can be tested on the MOC. Stretch goals include:
  - Implementing communication thread with input/outpur buffers for each party using pthreads (optimization)
  - Run a communication-intensive application using our Secrecy prototype on the UKL
  - Testing and benchmarking our prototype to compare performance gains against MPI performance
  
## 6. Release Planning
Release #1:

  - Remove dependencies on MPI from init function


Release #2:

  - Remove dependencies on MPI from init function
  - Establish standing TCP connection between 3 parties


Release #3:

  - Implement Master Orchestrator for socket connections in Secrecy
  - Implement TCP send/receive for data communication between parties
  - Remove other MPI dependencies


Release #4:

  - Integrate TCP send/receive functions into Secrecy codebase
  - Create test case to determine if TCP outputs match MPI outputs


Release #5:

  - Usage of a complex operator to provide an end to end test case
  - Perform performance testing of MPI vs. TCP implementations
  - Deployment of an instance on UKL and testing with QEMU

## DEMO 1

https://youtu.be/9HV23bVlr6E

## DEMO 2

https://youtu.be/NU8P3QJDxLk

## DEMO 3
https://youtu.be/zdv__MOggw0

## DEMO 4
https://youtu.be/Z-scyMytvbE

## DEMO 5
https://youtu.be/cFdX7Q8lbFE

## FINAL DEMO
https://drive.google.com/file/d/1_b7hpL80aKqERTxLinigbkGv5Yd2LahE/view

## Mentors
John Liagouris: liagos@bu.edu

Professor Orran Krieger: okrieg@bu.edu

Professor Peter Desnoyers: pjd-nu or pjd@ccs.neu.edu

Anqi Guo: anqianqi1


# SysX

SysX is a relational Multi-Party Computation framework based on replicated secret sharing.

This repository is organized as follows:
- The `src` folder contains the core functionality of Sysx, including the implementation of MPC primitives, relational oblivious operators, and party communication.
- The `examples` folder contains the implementation of example queries with the SysX API.
- The `test` folder contains various unit and end-to-end tests.
- The `experiments` folder contains the implementation of various microbenchmarks and performance experiments.
- Plotting scripts and other helper utilies are located in the `scripts` folder.
- Further documentation and detailed instructions for a setting up a cloud-based SysX depoyment are located in `docs`.

Build SysX
============
The following instructions assume a single-node OSX system. See [below](#specifying-dependencies-on-linux) for instructions on how to properly specify dependencies on Linux. To setup SysX on a Cloud environemnt, please see the [Cloud setup instructions](docs/MPI-setup.md).

To build SysX, you will need to install:
- C99
- [Libsodium](https://libsodium.gitbook.io/doc/installation)
- an MPI implementation, such as [OpenMPI](https://www.open-mpi.org/software/ompi/v4.0/) or [MPICH](https://www.mpich.org/downloads/).

Build and run the tests
------------
Change to the `tests` directory.

1. Build and run all tests: 
   - Run `make tests`. 

2. Build and run an individual test: 
   - Run `make test-xyz` to build a test, where `xyz` is the test name. For instance, run `make test-equality` to build the binary equality test. 
   - Execute the test with `mpirun -np 3 test-xyz`.

Run an example
---------
Change to the `examples` directory.

1. Build all examples: 
   - Run `make all`. 

2. Build and run an individual example, e.g. the comorbidity query: 
   - Build the example with `make comorbidity`.
   - Run the example with `mpirun -np 3 comorbidity <NUM_ROWS_1> <NUM_ROWS_2>`.

Run the experiments
---------
Change to the `experiments` directory.

1. Build all experiments: 
   - Run `make all`. 

2. Build and run an individual experiment, e.g. the equality microbenchmark: 
   - Build the experiment with `make exp-equality`.
   - Run it with `mpirun -np 3 exp-equality <INPUT_SIZE>`.

Specifying dependencies on Linux
-------------
To build and run SysX on linux, edit the provided `Makefile` as follows:
- Use the variables `CFLAGS= -03 -Wall` and `DEP= -lsodium -lm`
- Specify the dependency in the end of the target, for example:

    `exp-equality:   exp_equality.c $(PRIMITIVES) $(MPI) $(CFLAGS) -o exp-equality exp_equality.c $(PRIMITIVES) $(DEP)`
    
# Instructions for running exp-equality to generate Timing Plot

## These are the instructions for running exp-equality #3 with the sockets version of Secrecy

1. On your 3 VMs, ensure that Libsodium is installed correctly. If it is not, the experiment will not 
build.

2. Clone the ec528_secrecy repo from github and switch to the replace_MPI branch on all 3 VMs

3. If using VMs on the Mass Open Cloud, you may need to create and add a security group to each VM in order to allow TCP connections on Port 8000 as we have set for our experiment. To do this you must...

    - Go to your MOC Security Groups dashboard
    - Click the "Create Security Group" 
    - In the subsequent dialog, give your security group a name and click "Create Security Group"
    - You will see the new security group in the list of available security groups. Click the "Manage rules" button for that group.
    - On the next screen, click the "Add rule" button.
    - Enter 8000 in the "Port" field and click "Add" (you can leave the other fields with their default values)
    - Go to the MOC Instance Dashboard
    - From the menu at the upper right, select "Edit Security Groups"
    - Find the security group you created in the previous step listed under "All security groups". Click the "+" button to add it to "Instance security groups".
    - Click "Save".

4. On all 3 VMs, switch to the src folder and open the mpc_tcp.c file. At the top you will see definitions for RANK_ONE_IP, RANK_TWO_IP, and RANK_THREE_IP, you will need to change those string values to the respective IPs of each of your
VMs (and these need to be the exact same across all 3 VMs, i.e. RANK_ZERO_IP needs to be the same value in vm0, vm1 and vm2). The IPs should be each VMs respective eth0 ipv4 address when "ifconfig" is run. Our MOC VMs had eth0 IPs that started with 10.0.0. Choose a rank for each VM (0, 1, or 2), and input their respective eth0 IPs with the corresponding
variable for the IP. It is extremely important to remember which VM you designate as 0, 1, and 2 for later. 

5. On all 3 VMs, switch to the experiments folder, and run "make exp-equality." This will
build the experiment, with experiments 1,2, and 4 already commented out (there are a few compiler warnings, but they do
not affect the behavior of the executable).

6. Once the executable for the experiment is built, remember which VM you designated as rank 2, which VM you designated as rank 3, and which VM you designated as rank 1. This is the order you will have to run the executable. 

7. To run the experiment, run "./exp-equality RANK INPUT_SIZE" on each VM in the order specified in step 4 (i.e., vm1 --> vm2 --> vm0), where RANK is the respective VMs rank as designated in mpc_tcp.c (this is an integer value in [0,1,2]) and INPUT_SIZE is the size of the array for which you wish to run the experiment. The input size needs to be the same across all 3 VMs, but the rank is going to be unique to each VM. Running this in the specified order (vm1, vm2, vm0) will result in a successful run, and produce a text file named "tcp_timing.txt" in the experiment folder in each VM, which measures the latency of the eq_b_array() function in each party.

## Instructions to run exp-equality #3 using the MPI version of Secrecy
1. Install and configure Libsodium for each VM
2. Install and configure OpenMPI for each VM
3. Clone the repository that has the MPI version of Secrecy into each VM (For our repository, this branch is Secrecy-MPI)
4. CD to the experiments folder of the cloned repository
5. Create a text document in this folderthat acts as a hostfile that contains the three IP addresses of the parties that will be used. Each line of the hostfile should be constructed as ip-1:1 with the actual IP address replacing ip-1. Also, each party's ip address should be on a separate line in the hostfile.
6. Access the exp_equality.c file in the experiments folder and comment out the entirety of experiments 1,2,4, and 5 leaving only the ASYNC array-based equality experiment (exp-equality #3)
7. In the command line, run `mpirun --hostfile <name_of_hostfile> exp-equality <input_size>` using whatever input size you would like to test for. For our experiment, we tested input sizes of 2^10, 2^12, 2^14, 2^16, 2^18, 2^20. The timing for the experiment will be the last output of the experiment. For each test, this time should be documented into a text file that we chose to name mpi_timing.txt. This text file will be used to create the plot comparing the timing between the TCP and MPI implmentations of Secrecy.

## Steps to Generate Plot

1. Move the mpi_timing.txt file (in the experiments folder of the secrecy_MPI branch) into the same directory as the tcp_timing.txt file (which is located in the experiments folder of the replace_MPI branch), preferably into a directory not located on the secrecy_MPI branch or the replace_MPI branch. 

2. Move the secrecy_plot.py file located in the master branch of the repo into the same directory as the other 2.

3. Ensure python is installed, along with numpy and matplotlib. These libraries are needed to generate the plot. Ensure all python path variables are configured.

4. Run "python secrecy_plot.py" to generate plot

(Disclaimer, I do not know how to configure my own path variables for python, so I used Google Colab to generate the plot. To generate the plot on colab, simply upload the two text files containing the data, copy and paste the code in secrecy_plot.py into the notebook and run).

# Instructions for running complex operator to test TCP functionality

## Instructions to run exp_group_by_join_naive with the socket version of Secrecy
1.  Follow the instruction 1-4 for the instructions for running exp-equality #3 with the sockets version of Secrecy.
2.  On all 3 VMs, switch to the experiments folder, and run "make exp-group-by-join-naive" to build the experiment.
3.  To run the experiment, run "./exp-group-by-join-naive RANK NUM_ROWS_1 NUM_ROWS_2" on each VM in the order vm1 --> vm2 --> vm0, where RANK is the respective VMs rank as designated in mpc_tcp.c (this is an integer value in [0,1,2]) and NUM_ROWS is the size of the table for which you wish to run the experiment, which has the be a power of 2. The NUM_ROWS needs to be the same across all 3 VMs, but the rank is going to be unique to each VM. Running this in the specified order (vm1, vm2, vm0) will result in a successful run and prints the measurement of the latency of the experiment.

# Instructions for running orchestrator to fetch party IP addresses
1.  With 4 VMs on the MOC, designate one as the orchestrator, and 3 as the MPC parties
2.  Clone this repository onto all 4 VMs, and enable security groups for all 4 VMs to allow TCP communication via port 8000
3.  On the designated orchestrator party, run ifconfig and remember the ip address in the eth0 section (for our VMs it started with 10.0.0), this is needed for step 4.
4.  On the 3 MPC VMs, in the ipClient.c file located in the orchestrator directory, change the target IP address of the ipaddr structure in the connect() system call to the IP address of the orchestrator VM. 
5.  Once the ipClient.c files have been configured to connect to the IP of the orchestrator, compile the executable on all 3 VMs in the orchestrator directory with "gcc -std=c99 ipClient.c -o client", and compile the executable on the 4th orchestrator VM in the orchestrator directory with "gcc -std=c99 ipFetch.c -o fetch"
6.  On the orchestrator VM, run "./fetch" in the current directory
7.  On the 3 MPC VMs, run "./client" in the current directory
8.  In the orchestrator directory of all 3 MPC VMs, you should now see a ipAddress.txt file containing 3 lines with an IP address on each line

# Instructions for running secrecy on the UKL in qemu

![Alt Text](https://github.com/msisk23/MPC_Project/blob/main/UKL_1.png)
![Alt Text](https://github.com/msisk23/MPC_Project/blob/main/UKL_2.png)
![Alt Text](https://github.com/msisk23/MPC_Project/blob/main/UKL_3.png)
