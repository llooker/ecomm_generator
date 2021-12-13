This project contains the Scala classes used to generate theLook, our ecommerce dataset. This project has been setup to run on IntelliJ (a 30 day free trial of IntelliJ is available). 

## Local Setup Instructions

1. Clone the repo locally
git clone git@github.com:looker/ecommerce_demo_generator.git

2. Open up Repo on IntelliJ. This will load all the necessary files. 
File -> New -> Project from Existing Sources

3. Specify the Run Path
  Run -> Edit configurations <br/>
  Select Main Class which exists in the following location: com.looker.generator.Main and specify the working directory

4. Update Configurations in the main class to specify number of events and years to generate: <br/>
    ```Generator.generate(outputDirectory, 4, 1000)``` <br/>
    First Value (4) = Number of Years to Generate Data <br/>
    Second Value (1000) = Number of initial events per month

5. Run -> Run 'Main'

6. Generation should take 1-5 minutes depending on number of events generated. All data files and outputs can be found in 
    Project -> Data
    
## Making Updates

Submit any changes as Pull Requests to be approved by Admins of the GitHub project (Johnny Swenson)

To make any data updates in database to the Ecommerce Datasets, reach out to ops. You will need to generate a JAR File. 

To generate a JAR File: run ```sbt assembly``` in the project directory. The associated JAR file will be in ```/<Working Directory>/scala_ecommerce_generator/target/scala-2.11/```

Ops process to run the JAR File and related Data output streams: https://dig.looker.com/t/demo-generators-and-associated-etl/3420
