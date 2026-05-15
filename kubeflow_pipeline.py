"""Minimal Kubeflow Pipelines (KFP v2) example.

This script defines a tiny pipeline and compiles it to a YAML file.
"""

from kfp import dsl
from kfp.compiler import Compiler


@dsl.component
def get_message() -> str:
    return "hello from kubeflow"


@dsl.component
def print_message(message: str) -> None:
    print(message)


@dsl.pipeline(name="minimal-kubeflow-pipeline")
def minimal_pipeline() -> None:
    message = get_message()
    print_message(message=message.output)


if __name__ == "__main__":
    Compiler().compile(
        pipeline_func=minimal_pipeline,
        package_path="minimal_pipeline.yaml",
    )
