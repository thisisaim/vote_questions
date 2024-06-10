# Daily Questions and Votes Tracking Application

## Overview

This application tracks questions posted daily and maintains votes for each question, ensuring idempotency in voting. It leverages Apache Flink for stream processing and Python for the business logic.

## Prerequisites

- Apache Flink
- Python 3.x
- PyFlink

## Installation

1. **Install Apache Flink**: Follow the [official guide](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/try-flink/local_installation/) to install Flink on your machine.
2. **Install PyFlink**: Use pip to install PyFlink.
   ```bash
   pip install apache-flink

## Usage
Running the Application
Save the script to a file, for example, daily_questions.py.
Run the script using Python:

```bash
python daily_questions.py
```

Code Structure


- PostDailyQuestions Process Function:

- Schedules a timer to post questions daily at the specified time (question_schedule).
- The process_element method registers the first timer, and on_timer handles posting questions and scheduling the next timer.


TrackVotes Keyed Process Function:

- Ensures idempotency in voting by checking if a user has already voted for a question before recording their vote.


### Main Function:

- Sets up the execution environment and stream time characteristic.
- Initializes the PostDailyQuestions process function with a schedule.
- Processes votes, ensuring idempotency and tracking votes for each question.
- Prints the daily questions and processed votes.

Example Data
Initial Trigger Stream:
```json
[{"trigger": "start"}]
```

Questions:

```json
[
  {"question_id": 1, "question": "What is Flink?"},
  {"question_id": 2, "question": "What is Python?"}
]
```
Votes:

```json

[
  {"question_id": 1, "user_id": "user1", "vote": "A"},
  {"question_id": 1, "user_id": "user2", "vote": "B"},
  {"question_id": 1, "user_id": "user1", "vote": "A"}  // Duplicate vote should be ignored
]
```
