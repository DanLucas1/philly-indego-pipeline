FROM mageai/mageai:latest

ARG PROJECT_NAME=indego_pipeline
ARG USER_CODE_PATH=/home/src/

# Note: this overwrites the requirements.txt file in your new project on first run. 
# You can delete this line for the second run :) 

COPY ${PROJECT_NAME}/requirements.txt ${USER_CODE_PATH}requirements.txt 

RUN pip3 install -r ${USER_CODE_PATH}requirements.txt
