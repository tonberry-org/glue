# Local Dev

https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image

### Setup

1. Pull `amazon/aws-glue-lib` from docker hub:

   ```
   $ docker pull amazon/aws-glue-libs:glue_libs_3.0.0_image_01
   ```

2. Cofigure local workspace:

   ```
   $ WORKSPACE_LOCATION=/local_path_to_workspace
   $ SCRIPT_FILE_NAME=sample.py
   $ mkdir -p ${WORKSPACE_LOCATION}/src
   $ vim ${WORKSPACE_LOCATION}/src/${SCRIPT_FILE_NAME}
   ```

3. Run docker conainer
   ```
   $ docker run -it -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/jupyter_workspace/ \
       -e AWS_ACCESS_KEY_ID=<access-key> \
       -e AWS_SECRET_ACCESS_KEY=<aws-secret-access-key> \
       -e AWS_REGION=us-west-2 \
       -e AWS_DEFAULT_REGION=us-west-2 \
       -e DISABLE_SSL=true \
       --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 \
       --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01-arm64 \
       /home/glue_user/jupyter/jupyter_start.sh
   ```
4. Access server at `http://127.0.0.1:8888/`
