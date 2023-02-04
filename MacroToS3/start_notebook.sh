#!/bin/sh -eu
JUPYTER_WORKSPACE_LOCATION=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

docker run -it -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/jupyter_workspace/ \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e AWS_REGION=us-west-2 \
    -e AWS_DEFAULT_REGION=us-west-2 \
    -e DISABLE_SSL=true \
    --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 \
    --name glue_jupyter_lab \
    amazon/aws-glue-libs:glue_libs_3.0.0_image_01-arm64 \
    /home/glue_user/jupyter/jupyter_start.sh