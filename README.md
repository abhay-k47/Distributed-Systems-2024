# Distributed-Systems-2024

This repository contains all the assignments of the Distributed Systems Course (CS60002) offered at IIT Kharagpur in the Spring Semester of 2023-24. The course is taught by Prof. Sandip Chakraborty.

## Prerequisites

- Strong understanding of Algorithms and Data Structures
- Knowledge of computer networks and network protocols

## Getting Started

1. Clone the repository to your local machine:

```
git clone https://github.com/abhay-k47/Distributed-Systems-2024.git
```

2. Navigate to the specific assignment folder to access the problem statement, relevant code, and instructions to run the code.

## Description

### Assignment 1: Implementing a Customizable Load Balance

<!-- In this assignment, you have to implement a load balancer that routes the requests coming from several clients asynchronously among several servers so that the load is nearly evenly distributed among them. In order to scale a particular service with increasing clients, load balancers are used to manage multiple replicas of the service to improve resource utilization and throughput. In the real world, there are various use cases of such constructs in distributed caching systems, distributed database management systems, network traffic systems, etc.
To efficiently distribute the requests coming from the clients, a load balancer uses a consistent hashing data structure. The consistent hashing algorithm is described thoroughly with examples in Appendix A. You have to deploy the load balancer and servers within a Docker network as shown in Fig. 1. The load balancer is exposed to the clients through the APIs shown in the diagram (details on the APIs are given further). There should always be N servers present to handle the requests. In the event of failure, new replicas of the server will be spawned by the load balancer to handle the requests -->

- **Task**: Implement a load balancer that routes the requests coming from several clients asynchronously among several servers so that the load is nearly evenly distributed among them.
- **Concepts**: Consistent Hashing, Docker, REST APIs
- **Tools**: Docker, Flask, Python

### Assignment 2: Implementing a Scalable Database with Sharding

<!-- In this assignment, you have to implement a sharded database that stores only one table StudT in multiple shards distributed across several server containers. This is an incremental project so that you can reuse the codebase from the first assignment. A system diagram of the sharded database is shown in Fig. 1. Here, shards are subparts of the database that only manage a limited number of entries (i.e., shard size as shown in the diagram). Shards can be replicated across multiple server containers to enable parallel read capabilities. For this assignment, we assume that write requests are blocking for a particular shard. Thus, if two write requests are scheduled simultaneously on shard (i), one of them will wait for the other to complete.
However, Parallel writing to different shards, for instance, shard (i) and shard (j), is possible. The system’s current design provides scaling in two ways: (i) Read speed with more shard replicas and (ii) Database size with more shards and server -->

- **Task**: Implement a sharded database that stores only one table StudT in multiple shards distributed across several server containers.
- **Concepts**: Sharding, Replication, Consistency, Docker, REST APIs
- **Tools**: Docker, Flask, Python, MySQL

## Usage

1. Follow the instructions provided in each assignment folder to understand the task and requirements.
2. Run the code using the specified command or execution method.
3. Provide the necessary inputs as prompted or modify the code as required.
4. Analyze the output or observe the behavior as mentioned in the assignment guidelines.

## Troubleshooting

- If you encounter any issues while running the assignments, please feel free to contact us or raise an issue in this repository.

## References

- [Course Page](https://cse.iitkgp.ac.in/~sandipc/courses/cs60002/cs60002.html)
- [Docker](https://docs.docker.com/guides/get-started/)
- [Consistent Hashing](https://web.stanford.edu/class/cs168/l/l1.pdf)

## License

- The code in this repository is licensed under the [GNU GPLv3](https://choosealicense.com/licenses/gpl-3.0/) License.

## Contributors

- Abhay Kumar Keshari (20CS10001) [[abhay-k47](https://github.com/abhay-k47)]
- Morreddigari Likhith Reddy (20CS10037) [[likhnic](https://github.com/likhnic)]
- Shivansh Shukla (20CS10057) [[shivansh1102](https://github.com/shivansh1102)]
- Abhijeet Singh (20CS30001) [[abhijeetsingh13](https://github.com/abhijeetsingh13)]
