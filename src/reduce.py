# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.

import os
from pathlib import Path
from mldesigner import command_component, Input, Output

from core.reducer import *
from core.structure import *

@command_component(
    name="reduce",
    version="1",
    display_name="Reduce Problem",
    description="Reduce the problem space by some heuristic",
    environment=dict(
        conda_file=Path(__file__).parent / "env.yaml",
        image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04",
    ),
)
def reduce_component(
    model_input: Input(type="uri_folder"),
    distance: Input(type="uri_folder"),
    model_result_partial: Output(type="uri_folder"),
    model_input_reduced: Output(type="uri_folder"),
):
    """Main function of the reduce script."""

    # parser = argparse.ArgumentParser("reduce")

    # parser.add_argument("--model_input", type=str, help="the complete list of model input")
    # parser.add_argument("--distance", type=str, help="the distance file")
    # parser.add_argument("--model_result_partial", type=str, help="partital result after reduction")
    # parser.add_argument("--model_input_reduced", type=str, help="the reduced model input")


    # args = parser.parse_args()

    # print("Argument 1: %s" % args.model_input)
    # print("Argument 2: %s" % args.distance)
    # print("Argument 3: %s" % args.model_result_partial)
    # print("Argument 4: %s" % args.model_input_reduced)

    # # Get the experiment run context
    # # run = Run.get_context()
    # # model_input = run.input_datasets['model_input'] 
    # # distance = run.input_datasets['distance'] 

    # # model_input_file = glob.glob(model_input + "/*.csv")[0]
    # # distance_file =  glob.glob(distance + "/*.csv")[0]

    ## Initialize the input
    reducer = SearchSpaceReducer()
    model_input_origin = ModelInput()
    model_input_origin.initInputFromFile(model_input, distance)

    ## Reduce process
    model_input_reduced, model_result_partial = reducer.reduce1(model_input_origin)
    #model_input_reduced, model_result_partial = reducer.reduce2(model_input_origin)

    ## Save the results

    os.makedirs(model_result_partial)
    os.makedirs(model_input_reduced)

    model_input_reduced.toOrderDF().to_csv(os.path.join(model_input_reduced, "order_reduced.csv"), index=False)
    model_result_partial.toScheduleDF().to_csv(os.path.join(model_result_partial, "model_result_partial.csv"), index=False)
