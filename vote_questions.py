import json
import time
from datetime import datetime, timedelta

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    MapFunction,
    ProcessFunction,
    TimerService,
)


class PostDailyQuestions(ProcessFunction):
    def __init__(self, question_schedule):
        self.question_schedule = question_schedule

    def open(self, runtime_context):
        self.questions = [
            {"question_id": 1, "question": "Buy a house with HOA?"},
            {"question_id": 2, "question": "Manual or automatic transmission?"},
        ]
        self.current_index = 0

    def process_element(self, value, ctx: "ProcessFunction.Context"):
        next_time = self.get_next_schedule_time()
        ctx.timer_service().register_event_time_timer(next_time)

    def on_timer(self, timestamp, ctx: "ProcessFunction.OnTimerContext"):
        if self.current_index < len(self.questions):
            question = self.questions[self.current_index]
            ctx.output(Types.STRING(), json.dumps(question))
            self.current_index += 1
            next_time = self.get_next_schedule_time()
            ctx.timer_service().register_event_time_timer(next_time)

    def get_next_schedule_time(self):
        now = datetime.now()
        next_run = now.replace(
            hour=self.question_schedule.hour,
            minute=self.question_schedule.minute,
            second=0,
            microsecond=0,
        )
        if now >= next_run:
            next_run += timedelta(days=1)
        return int(time.mktime(next_run.timetuple()) * 1000)


class TrackVotes(KeyedProcessFunction):
    def open(self, runtime_context):
        self.votes = {}

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        record = json.loads(value)
        question_id = record["question_id"]
        user_id = record["user_id"]
        vote = record["vote"]

        if question_id not in self.votes:
            self.votes[question_id] = {}

        if user_id not in self.votes[question_id]:
            self.votes[question_id][user_id] = vote

        ctx.output(Types.STRING(), json.dumps(self.votes))


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    initial_stream = env.from_collection([json.dumps({"trigger": "start"})])

    question_schedule = datetime.now().replace(
        hour=9, minute=0, second=0, microsecond=0
    )
    daily_questions_stream = initial_stream.process(
        PostDailyQuestions(question_schedule)
    )

    vote_stream = env.from_collection(
        [
            '{"question_id": 1, "user_id": "user1", "vote": "A"}',
            '{"question_id": 1, "user_id": "user2", "vote": "B"}',
            '{"question_id": 1, "user_id": "user1", "vote": "A"}',
        ]
    )

    processed_votes = vote_stream.key_by(
        lambda x: json.loads(x)["question_id"]
    ).process(TrackVotes())

    daily_questions_stream.print()
    processed_votes.print()

    env.execute("Daily Questions and Votes Tracking")


if __name__ == "__main__":
    main()
