DOCKER_IMAGE=quay.io/utilitywarehouse/mobiq

docker-image:
	docker build -t $(DOCKER_IMAGE) .

docker-push:
	docker push $(DOCKER_IMAGE)