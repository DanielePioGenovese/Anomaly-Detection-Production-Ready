FROM redis:6.2-alpine

# Set the working directory
WORKDIR /usr/local/etc/redis

# Copy configuration file
COPY config/redis.conf /usr/local/etc/redis/redis.conf

# Expose Redis port
EXPOSE 6379

# Start Redis with the specific config
CMD ["redis-server", "/usr/local/etc/redis/redis.conf"]
