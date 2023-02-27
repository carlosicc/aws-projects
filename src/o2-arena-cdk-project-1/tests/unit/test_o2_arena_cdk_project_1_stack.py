import aws_cdk as core
import aws_cdk.assertions as assertions

from o2_arena_cdk_project_1.o2_arena_cdk_project_1_stack import O2ArenaCdkProject1Stack

# example tests. To run these tests, uncomment this file along with the example
# resource in o2_arena_cdk_project_1/o2_arena_cdk_project_1_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = O2ArenaCdkProject1Stack(app, "o2-arena-cdk-project-1")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
