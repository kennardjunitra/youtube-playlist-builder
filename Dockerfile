FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#Install datadog library
RUN pip install datadog-lambda


# 4. Add the Datadog Lambda Extension (from Datadog’s public ECR)
COPY --from=public.ecr.aws/datadog/lambda-extension:latest /opt/ /opt/

COPY app ./app


# Lambda handler module path
CMD ["datadog_lambda.handler.handler"]

