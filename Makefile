SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")

# https://github.com/opencontainers/image-spec/blob/master/annotations.md
BUILD_DATE := $(shell date -u +%FT%TZ)
VERSION ?= $(shell git rev-parse --short HEAD)
RELEASE_VERSION ?= $(shell git describe --exact-match --tags)
DOCKER_IMAGE := stellar/stellar-galexie

docker-build:
	$(SUDO) docker build --platform linux/amd64 --pull --label org.opencontainers.image.created="$(BUILD_DATE)" \
    --build-arg GOFLAGS="-ldflags=-X=github.com/stellar/stellar-galexie/internal.version=$(VERSION)" \
$(if $(STELLAR_CORE_VERSION), --build-arg STELLAR_CORE_VERSION=$(STELLAR_CORE_VERSION)) \
	-f docker/Dockerfile \
	-t $(DOCKER_IMAGE):$(VERSION) .

docker-clean:
	$(SUDO) docker stop fake-gcs-server || true
	$(SUDO) docker rm fake-gcs-server || true
	$(SUDO) rm -rf ${PWD}/storage || true
	$(SUDO) docker network rm test-network || true
    
docker-test-fake-gcs: docker-clean
	# Create temp storage dir
	$(SUDO) mkdir -p ${PWD}/storage/galexie-test

	# Create test network for docker
	$(SUDO) docker network create test-network

	# Run the fake GCS server
	$(SUDO) docker run -d --name fake-gcs-server -p 4443:4443 \
		 -v ${PWD}/storage:/data --network test-network fsouza/fake-gcs-server -scheme http

	# Run
	$(SUDO) docker run --platform linux/amd64 -t --network test-network \
		-v ${PWD}/docker/config.test.toml:/config.toml \
		-e STORAGE_EMULATOR_HOST=http://fake-gcs-server:4443 \
		$(DOCKER_IMAGE):$(VERSION) \
		scan-and-fill --start 1000 --end 2000

	$(MAKE) docker-clean

docker-pull:
	$(SUDO) docker pull $(DOCKER_IMAGE):$(VERSION)

docker-push:
	$(SUDO) docker push $(DOCKER_IMAGE):$(VERSION)

docker-push-release:
	$(SUDO) docker tag $(DOCKER_IMAGE):$(VERSION) $(DOCKER_IMAGE):$(RELEASE_VERSION)
	$(SUDO) docker push $(DOCKER_IMAGE):$(RELEASE_VERSION)

docker-push-latest:
	$(SUDO) docker tag $(DOCKER_IMAGE):$(VERSION) $(DOCKER_IMAGE):latest
	$(SUDO) docker push $(DOCKER_IMAGE):latest
