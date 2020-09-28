package cvmfs

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/docker/distribution/manifest/schema2"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	storagemiddleware "github.com/docker/distribution/registry/storage/driver/middleware"
)

func init() {
	storagemiddleware.Register("cvmfs", storagemiddleware.InitFunc(newCVMFSStorageMiddleware))
}

type CVMFSStorageMiddleware struct {
	storagedriver.StorageDriver
	workingOnLock *sync.Mutex
	workingOn     map[string]chan bool
}

func newCVMFSStorageMiddleware(storageDriver storagedriver.StorageDriver, options map[string]interface{}) (storagedriver.StorageDriver, error) {
	return CVMFSStorageMiddleware{storageDriver, &sync.Mutex{}, make(map[string]chan bool)}, nil
}

func (s *CVMFSStorageMiddleware) StoreLayer(digest string) {
	ch := make(chan bool) // we never push here
	s.workingOnLock.Lock()
	s.workingOn[digest] = ch
	s.workingOnLock.Unlock()

	// actually doing the ingestion

	// defer order, first we close the channel to communicate that we have done with the layer
	// then we remove the layer from the map
	defer func() {
		s.workingOnLock.Lock()
		delete(s.workingOn, digest)
		s.workingOnLock.Unlock()
	}()
	defer func() { close(ch) }()
}

func (s *CVMFSStorageMiddleware) CheckLayer(digest string) {
	s.workingOnLock.Lock()
	ch, ok := s.workingOn[digest]
	s.workingOnLock.Unlock()

	// we wait until the channel does not get closed
	if ok {
		<-ch
	}

	// check if the layer is in the FS
}

// on Stat make sure the layer is in CVMFS, if not, push it.

func (s CVMFSStorageMiddleware) PutContent(ctx context.Context, path string, content []byte) error {
	fmt.Println("\nmiddleware path: ", path)
	tokens := strings.Split(path, "/")
	if tokens[len(tokens)-2] == "current" {
		tag := tokens[len(tokens)-3]
		fmt.Println("Indentified tag: ", tag)
		repository := strings.Join(tokens[5:len(tokens)-5], "/")
		fmt.Println("Got repository: ", repository)
		manifestBlob := string(content[7:])
		fmt.Println("Got blob reference: ", manifestBlob)

		manifestPath := filepath.Join("/", "docker", "registry", "v2", "blobs", "sha256", manifestBlob[0:2], manifestBlob, "data")
		fmt.Println("Got path of the manifest: ", manifestPath)
		manifestBytes, err := s.GetContent(ctx, manifestPath)

		manifest := schema2.DeserializedManifest{}
		err = manifest.UnmarshalJSON(manifestBytes)

		if err != nil {
			return fmt.Errorf("Error in getting the manifest")
		}

		for _, layer := range manifest.Layers {
			// we want to make sure all the layers are in the FS
			// what if a layer is not?
			// will that layer be pushed?
			// it is waiting?
			// did it fail?

			// layer is in the FS -> OK
			// layer is not in the FS
			// -- we are working on the layer -> Wait the work to finish
			// -- we are not working on the layer
			// -- -- the layer is inside the docker registry -> push it into cvmfs
			// -- -- the layer is not inside the docker registry -> fail how did we ended up here?
			// -- the work fails!
			// -- -- push from the registry again
			fmt.Println("Checking on layer: ", layer)
		}

		// unmarshal the bytes into a manifest
		// some way to manage the error
		// push the metadata

		// make sure all layers are there, wait otherwise
		// wait what? Is the layer ever going to be there
		// should we keep track of what we are doing?

		// create the flat FS?
		// ... how ...

	}
	return s.StorageDriver.PutContent(ctx, path, content)
}

type CVMFSWriter struct {
	storagedriver.FileWriter
	path            string
	isClosedAlready bool
}

func (w *CVMFSWriter) Write(p []byte) (int, error) {
	return w.FileWriter.Write(p)
}

func (w *CVMFSWriter) Close() error {
	if w.isClosedAlready {
		return nil
	}
	w.isClosedAlready = true
	return w.FileWriter.Close()
}

func (s CVMFSStorageMiddleware) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	fmt.Printf("\nNew writer! %s\n\n", path)
	writer, err := s.StorageDriver.Writer(ctx, path, append)
	if err != nil {
		return writer, err
	}
	return &CVMFSWriter{writer, path, false}, nil
}

func (s CVMFSStorageMiddleware) Move(ctx context.Context, source, dest string) error {
	fmt.Printf("\tMoving from %s -> %s\n", source, dest)
	layer, err := getLayerName(dest)
	if err != nil {
		return s.StorageDriver.Move(ctx, source, dest)
	}
	fmt.Printf("\n\tFound layer: %s", layer)
	reader, err := s.Reader(ctx, source, 0)
	// the url should be in the config
	url := fmt.Sprintf("http://localhost:8080/layer/filesystem/%s", layer)
	resp, err := http.Post(url, "", reader)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	}
	defer resp.Body.Close()
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	}
	fmt.Printf("http response: %s\n", string(bytes))
	return s.StorageDriver.Move(ctx, source, dest)
}

func getLayerName(path string) (string, error) {
	tokens := strings.Split(path, "/")
	var i int
	for j, token := range tokens {
		if token == "sha256" {
			i = j + 2
		}
	}
	if i != 0 {
		return tokens[i], nil
	}
	return "", fmt.Errorf("not found layer hash")
}
