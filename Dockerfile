FROM apache/spark-py

# UPDATE PACKAGE
USER root
SHELL ["/bin/bash", "--login", "-c"]
RUN apt-get update

# INSTALL CURL
RUN apt-get install -y curl

# INSTALL ZIP
RUN apt-get install -y zip

# INSTALL NODE
ENV NODE_VERSION=18
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.2/install.sh | bash
ENV NVM_DIR=/root/.nvm
# RUN [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh" # This loads nvm
RUN nvm install ${NODE_VERSION}
RUN nvm use v${NODE_VERSION}
RUN nvm install-latest-npm


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
CMD ["./runLoop.sh"]