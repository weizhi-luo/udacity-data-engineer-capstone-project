FROM public.ecr.aws/lambda/python:3.9

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
COPY data_conversion.py ${LAMBDA_TASK_ROOT}
COPY data_model.py ${LAMBDA_TASK_ROOT}
COPY file_access.py ${LAMBDA_TASK_ROOT}
COPY file_tool.py ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]