FROM apache/spark-py

# UPDATE PACKAGE
USER root
RUN apt-get update

# INSTALL CURL
RUN apt-get install -y curl

# INSTALL ZIP
RUN apt-get install -y zip

# INSTALL NODE
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - 
RUN apt-get install -y nodejs

ENV SPARK_LOCAL_HOSTNAME=localhost

# INSTALL AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# INSTALL GA VOTES BACKEND DEPS
# only copy deps so this can act as a layer/faster rebuild
RUN mkdir /opt/gavotesBackend
WORKDIR /opt/gavotesBackend
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY package.json .
RUN npm install

# INSTALL GA VOTES
COPY . /opt/gavotesBackend/.

# CREATE DATA DIRS
RUN mkdir /opt/gavotesBackend/data
RUN mkdir /opt/gavotesBackend/data/frontend

# BAD HACK
RUN mkdir -p /opt/gavotesBackend/data/geojson/precincts/2020_simple/
COPY dataHacky/GA_precincts_id_to_name.csv /opt/gavotesBackend/data/geojson/precincts/2020_simple/GA_precincts_id_to_name.csv
COPY dataHacky/2022_runoff_map_ids_manual.csv /opt/gavotesBackend/data/electionResults/2022_runoff/2022_runoff_map_ids_manual.csv
# SET DEFAULT ENTRY POINT
CMD ["/bin/sh", "-c" , "echo 127.0.0.1 $HOSTNAME >> /etc/hosts && ./runLoop.sh"]
# CMD ["./runLoop.sh"]