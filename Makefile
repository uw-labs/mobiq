DOCKER_IMAGE=quay.io/utilitywarehouse/mobiq

docker-image:
	docker build -t $(DOCKER_IMAGE):$(shell cat package.json | jq -r .version) .

docker-push:
	docker push $(DOCKER_IMAGE)
