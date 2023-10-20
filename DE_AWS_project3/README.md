# Project: STEDI Human Balance Analytics

## Introduction

Spark and AWS Glue allow you to process data from multiple sources, categorize the data, and curate it to be queried in the future for multiple purposes. As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model. 

## Project Details

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Implementation

### Landing Zone

**Glue Tables**:
Use Athena to create Glue tables, use these sql scripts:
*Customer Landing*:
- [customer_landing.sql](scripts/customer_landing.sql)
<figure>
  <img src="images/customer_landing.png" alt="Customer Landing data" width=60% height=60%>
</figure>

*Accelerometer Landing*:

- [accelerometer_landing.sql](scripts/accelerometer_landing.sql)

<figure>
  <img src="images/accelerometer_landing.png" alt="Accelerometer Landing data" width=60% height=60%>
</figure>

*Step trainer Landing*:

- [step_trainer_landing.sql](scripts/step_trainer_landing.sql)

<figure>
  <img src="images/step_trainer_landing.png" alt="Step trainer Landing data" width=60% height=60%>
</figure>

### Trusted Zone

**Glue job scripts**:
Use glue studio to create these scripts:
<figure>
  <img src="images/accelerometer_trust_zone.png" alt="accelerometer_trust_diagram" width=60% height=60%>
</figure>

- [customer_landing_to_trusted.py](scripts/customer_landing_to_trusted.py)
- [accelerometer_landing_to_trusted_zone.py](scripts/accelerometer_landing_to_trusted.py)
- [step_trainer_landing_to_trusted_zone.py](scripts/step_trainer_landing_to_trusted.py)

and click run to run these glue jobs
<figure>
  <img src="images/job_accelerometer_trust.png" alt="job_accelerometer_trust" width=60% height=60%>
</figure>

**Use Athena to show results**:
Trusted Zone Query results:

<figure>
  <img src="images/customer_trusted.png" alt="Customer Truested data" width=60% height=60%>
</figure>

<figure>
  <img src="images/accelerometer_trusted.png" alt="accelerometer_trusted_data" width=60% height=60%>
</figure>

<figure>
  <img src="images/step_trainer_trusted.png" alt="step_trainer_trusted_data" width=60% height=60%>
</figure>

### Curated Zone

**Glue job scripts**:
- [customer_trusted_to_curated.py](scripts/customer_trusted_to_curated.py)

<figure>
  <img src="images/customer_curated.png" alt="customer_curated data" width=60% height=60%>
</figure>

- [trainer_trusted_to_curated.py](scripts/trainer_trusted_to_curated.py)

<figure>
  <img src="images/machine_learning_curated.png" alt="Machine learning curated data" width=60% height=60%>
</figure>
