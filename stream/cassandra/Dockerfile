# Utiliser l'image officielle de Cassandra
FROM cassandra:latest

# Copier le script CQL dans l'image
COPY stream/cassandra/init.cql /docker-entrypoint-initdb.d/



# Start Cassandra in the foreground
CMD ["cassandra", "-f"]