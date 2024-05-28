from typing import Any, Optional, List
from abc import ABC, abstractmethod
import pandas as pd


class PipelineStep(ABC):
    """
    Abstract base class for a pipeline step.
    """
    def __init__(self, name: str):
        """
        Initialize the pipeline step.

        :param name: Name of the step
        """
        self.name = name

    @abstractmethod
    def function(self, data: Optional[pd.DataFrame], **kwargs) -> pd.DataFrame:
        """
        Abstract method to be implemented in concrete classes.

        :param data: Input DataFrame or None
        :return: DataFrame after the step is applied
        """
        pass

    def execute(self, data: Optional[pd.DataFrame] = None, **kwargs) -> pd.DataFrame:
        """
        Execute the step.

        :param data: Input DataFrame or None
        :return: DataFrame after the step is applied
        """
        print(f"Executing step: {self.name}")
        return self.function(data, **kwargs)


class Pipeline:
    """
    Class representing a data processing pipeline.
    """
    def __init__(self, steps: List[PipelineStep]):
        """
        Initialize the pipeline with a list of steps.

        :param steps: List of PipelineStep objects
        """
        self.steps = steps

    def execute(self, initial_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Execute all steps in the pipeline sequentially.

        :param initial_data: Initial DataFrame or None
        :return: Final DataFrame after all steps are applied
        """
        data = initial_data
        for step, kwargs in self.steps:
            data = step.execute(data, **kwargs)
        return data


# Define the PipelineBuilder class
class PipelineBuilder:
    """
    Builder class to construct a data processing pipeline.
    """
    def __init__(self):
        """
        Initialize the builder.
        """
        self.steps = []

    def add_step(self, step: PipelineStep, **kwargs) -> 'PipelineBuilder':
        """
        Add a step to the pipeline.

        :param step: PipelineStep object
        :return: PipelineBuilder object
        """
        self.steps.append((step, kwargs))
        return self  # Enables chaining

    def build(self) -> Pipeline:
        """
        Build the pipeline.

        :return: Pipeline object
        """
        return Pipeline(self.steps)
