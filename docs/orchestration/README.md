Modern, cloud-native applications are designed around the needs of various tenants. A Minio instance best serves the needs of a 
single tenant. As and when required, you can spin new Minio instances to handle the needs of new tenant. With recent advancements in 
DevOps and Cloud deployment strategies, it doesn't make sense for Minio to manage the infrastructure it is running on. That task can be 
safely handed over to orchestration tools like Docker Swarm. 

This is why, Minio is designed to work in conjunction with external orchestrators. Not only this enables keeping each tenant relatively 
small and thus limit the failure domain, it also makes deploying new Minio instance as easy as launching a [Swarm service](https://docs.docker.com/engine/swarm/key-concepts/#/services-and-tasks). This ensures that as you scale, the complexity doesn't scale proportionately. 
