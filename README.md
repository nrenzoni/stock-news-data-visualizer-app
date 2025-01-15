This repo contains the streamlit app hosted
at [stock-news-visualizer-v1](https://stock-news-visualizer-v1.streamlit.app).

The app visualizes structured data extracted from stock news articles, including:

* sentiment analysis
* similarity analysis
* article summary embeddings
* stock symbol mentions
* article publication frequency
* stock price data

I've had the idea for this project for a while. Many thanks to the Airbyte / Motherduck competition which motivated me
to actually do it!

Throughout this project, effort went into the following:

* Extracting structured data from unstructured text using LLM technology
* Retrieving stock price data for relevant stock symbols mentioned in the data set news articles
* Generating embeddings for article summaries for similarity analysis
* Setting up a self-hosted MongoDb cluster as a data source for Airbyte
* Configuring Airbyte to sync data from MongoDb to Motherduck
* Building a streamlit app to visualize the extracted data

Motherduck connection string for the database:
`ATTACH 'md:_share/stock_news_sentiment_and_similarity_db/f6d399bb-1f34-4ee7-90aa-c76da12ea3ed'`

Ancillary repos built for this project:

* [stock-news-llm-scripts](https://github.com/nrenzoni/stock-news-llm-scripts) - a set of scripts for:
    * ETL between MongoDb, DuckDb, and ClickhouseDb - prior to this project, I had news article data sitting on MongoDb
      and ClickhouseDb. These scripts contain functionality to transfer the data to a fresh MongoDb which served as a '
      mock' OLTP database for Airbyte to sync to Motherduck. For this project, I ran the scripts manually. However in
      production, the scripts can be scheduled using Airflow or similar.
    * requesting textual embedding from the `sentence-embedding-server` (below)
    * OHLCV downloader for stock symbols mentioned in the news articles

* [sentence-embedding-server](https://github.com/nrenzoni/sentence-embedding-server) - a server to calculate sentence
  embeddings, used for similarity analysis of the article summaries

MongoDb as OLTP
---

* News articles (unstructured text)
* extracted structure - …
* 256 dim embedding for article summary
* Price and trade volume data on stock symbols mentioned the articles

Airbyte
---

* Easily Syncs data from MongoDb to Motherduck
* Ideally scheduled to sync at the end of every day

Motherduck
---

* data hosting provider of DuckDb + user-friendly web UI, efficient for data analysis and exploration

**Screenshots**:

MongoDb extracted structed features from web-scraped news articles:

![Screenshot 2025-01-14 170929](https://github.com/user-attachments/assets/95e73a39-a0a4-4c37-b9f4-524dd7b126e3)

Airbyte setup of MongoDb source (auto detects source schema, and handles changes):

![Screenshot 2025-01-14 170718](https://github.com/user-attachments/assets/fd42b9c4-9db1-48c2-bed5-7afd662c545d)

Airbyte sync progress screen:

![Screenshot 2025-01-14 170820](https://github.com/user-attachments/assets/ae478f10-7e9e-41f2-b2c9-106aea4b43aa)

MotherDuck web UI 

![Screenshot 2025-01-14 171051](https://github.com/user-attachments/assets/c900f66d-785f-47cc-b6c1-393cef5a0b25)

---

technical challenges:

\* The biggest challenge I faced doing this project was setting up a self-hosted MongoDb cluster compatible with Airbyte.
It took me 15 days to get it working properly! The multistep process and experimentation included:

- enabling TLS on MongoDb - I had to generate a Let’s Encrypt signed certificate using the CertBot tool, and DuckDNS for
  redirecting to a self-hosted IP address.
- Using the generated certificate in the MongoDb configuration file and docker image.
- Experimenting with connecting the Airbyte self-hosted cluster on KinD (Kubernetes on Ducker) to the same network as
  the
  MongoDb Docker.
- Airbyte requires that the MongoDb be a replica set. To set up a single node replica set on MongoDb, the hostname used
  in the replica set initialization function on MongoDb must match the TLS hostname. If not (e.g. using the localhost),
  then MongoDb doesn’t show a direct error and instead the
  error appears when connecting with the replica set name in the connection string, but without it, the connection works
  fine.
- Experimenting with port mapping from my home router to the host running MongoDb. After much trial and error, including
  trying to
  get Airbyte on KinD to use the docker host machine’s /etc/hosts file, looking into setting up a hosts file on the KinD
  cluster, and a
  couple other things, I had a lightbulb moment. I added a DNS mapping entry on my home router (running OPNSense)
  to map the TLS hostname to the localhost and Airbyte was able to connect to the MongoDb replica set and successfully
  ingest data.
