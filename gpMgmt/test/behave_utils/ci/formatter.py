import os

from behave.formatter.base import Formatter
from behave.model import ScenarioOutline
from behave.model_core import Status


def before_scenario(scenario):
    pass


def after_scenario(scenario):
    if scenario.status == Status.failed:
        try:
            import allure
        except ImportError:
            pass
        else:
            if scenario.captured.output:
                allure.attach(scenario.captured.output, name="stdout/stderr")
            logs_path = os.path.join(os.path.expanduser("~"), "gpAdminLogs")
            files = os.listdir(logs_path)
            for file in files:
                if os.path.isfile(os.path.join(logs_path, file)):
                    allure.attach.file(os.path.join(logs_path, file), name=file)


def wrap_scenario(scenario):
    def inner(func):
        def wrapped_func(*args, **kwargs):
            before_scenario(scenario)
            result = func(*args, **kwargs)
            after_scenario(scenario)
            return result
        return wrapped_func
    return inner


class CustomFormatter(Formatter):

    def _wrap_scenario(self, scenarios):
        for scenario in scenarios:
            if isinstance(scenario, ScenarioOutline):
                self._wrap_scenario(scenario)
            else:
                scenario.run = wrap_scenario(scenario)(scenario.run)

    def feature(self, feature):
        self._wrap_scenario(feature.scenarios)
