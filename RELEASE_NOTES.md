## RELEASE NOTES
_samba-chassis_, _v0.3.1_, 2018-12-20

### Overview
This release improves the logging package and adds
better logging for the tasks module.

The jobs package was removed. An improved version
of it is planned for future releases.

Documentation was improved and templates were created.

### Issues 
#### Tasks Logging
Logging for the tasks package was missing some 
crucial data to identify bugs our clients were
experiencing.
##### Resolution 
The logging package was improved with the concept
of job that our microservices use. The tasks
logging statements were also improved to use this
concept.

### What's new
* The logging package now has a class called 
ServiceLogger that always logs job id ans name. 
* The tasks logging statements were improved to use
the new logger class.
* New template for README, HISTORY and RELEASE_NOTES.
* Unified README to root dir.
* Removed jobs package.

### End-User Impact 
The end user will only have to upgrade if the
new features can be of you for their use case.

### Support Impacts 
No support impact.

### Installation 
The installation process remanis the same.

### Upgrade
To upgrade from the previous version is necessary to:
- Remove any reference for the job_tracker package.
- Change logging uses to the logging package new 
getLogger function.
- The run tasks statements must be changed to use
the task_pool argument.
- Config must be updated in the section tasks to use
the task-pool instead of service name and project.  
 
### Contact 
Vitor Paisante <vitor.paisante@sambatech.com>