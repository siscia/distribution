package cvmfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/distribution/manifest/schema2"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	storagemiddleware "github.com/docker/distribution/registry/storage/driver/middleware"
	"github.com/opencontainers/image-spec/specs-go/v1"
)

func init() {
	storagemiddleware.Register("cvmfs", storagemiddleware.InitFunc(newCVMFSStorageMiddleware))
}

type blobType int

const (
	unknowBT   blobType = 0 // match the zero value of int, 0
	manifestBT          = iota + 1
	configBT
	layerBT
	unrecognizedBT // it is not a layer (not gziped file) nor we where able to unmarshal in a manifest or in a config file
)

type CVMFSStorageMiddleware struct {
	storagedriver.StorageDriver
	blobTypesMutex sync.Mutex
	blobTypes      map[string]blobType
}

func newCVMFSStorageMiddleware(storageDriver storagedriver.StorageDriver, options map[string]interface{}) (storagedriver.StorageDriver, error) {
	fmt.Println("New storage middleware")
	driver := &CVMFSStorageMiddleware{storageDriver, sync.Mutex{}, make(map[string]blobType)}
	go driver.periodicCleanUpBlobTypes()
	return driver, nil
}

func (s *CVMFSStorageMiddleware) periodicCleanUpBlobTypes() {
	for {
		<-time.After(60 * time.Second)
		func() {
			s.blobTypesMutex.Lock()
			defer s.blobTypesMutex.Unlock()
			if len(s.blobTypes) >= 1000 { // why so many?
				s.blobTypes = make(map[string]blobType)
			}
		}()
	}
}

func (s *CVMFSStorageMiddleware) getHost() string {
	return "localhost:8080"
}

func (s *CVMFSStorageMiddleware) checkLayer(digest string) int {
	url := fmt.Sprintf("http://%s/layer/status/%s", s.getHost(), digest)
	resp, err := http.Get(url)
	if err != nil {
		return 500
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func (s *CVMFSStorageMiddleware) pushLayer(digest string, layerData io.Reader) (*http.Response, error) {
	url := fmt.Sprintf("http://%s/layer/filesystem/%s", s.getHost(), digest)
	return http.Post(url, "", layerData)
}

func (s *CVMFSStorageMiddleware) createFlat(name string, manifest io.Reader) (*http.Response, error) {
	url := fmt.Sprintf("http://%s/flat/human/%s", s.getHost(), name)
	return http.Post(url, "", manifest)
}

func (s *CVMFSStorageMiddleware) addBlobType(digest string, bT blobType) {
	s.blobTypesMutex.Lock()
	defer s.blobTypesMutex.Unlock()
	s.blobTypes[digest] = bT
}

func (s *CVMFSStorageMiddleware) getBlobType(digest string) blobType {
	s.blobTypesMutex.Lock()
	defer s.blobTypesMutex.Unlock()
	return s.blobTypes[digest]
}

// on Stat make sure the layer is in CVMFS, if not, push it.
func (s *CVMFSStorageMiddleware) PutContent(ctx context.Context, path string, content []byte) error {
	fmt.Println("\nmiddleware path: ", path)
	tokens := strings.Split(path, "/")
	if tokens[len(tokens)-2] == "current" {
		tag := tokens[len(tokens)-3]
		fmt.Println("Indentified tag: ", tag)
		repository := strings.Join(tokens[5:len(tokens)-5], "/")
		fmt.Println("Got repository: ", repository)
		manifestBlob := string(content[7:])
		s.addBlobType(manifestBlob, manifestBT)
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
			s.addBlobType(layer.Digest.String(), layerBT)
		}

		manifestBytes2, err := manifest.MarshalJSON()
		if err != nil {
			return err
		}
		name := fmt.Sprintf("%s:%s", repository, tag)
		resp, err := s.createFlat(name, bytes.NewReader(manifestBytes2))
		if err != nil {
			fmt.Printf("Error: %s\n", err.Error())
		}
		defer resp.Body.Close()
		bytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("Error: %s\n", err.Error())
		}
		fmt.Printf("http response: %s\n", string(bytes))

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

func (s *CVMFSStorageMiddleware) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	fmt.Printf("\nNew writer! %s\n\n", path)
	writer, err := s.StorageDriver.Writer(ctx, path, append)
	if err != nil {
		return writer, err
	}
	return &CVMFSWriter{writer, path, false}, nil
}

func (s *CVMFSStorageMiddleware) Move(ctx context.Context, source, dest string) error {
	fmt.Printf("\tMoving from %s -> %s\n", source, dest)
	layer, err := getLayerName(dest)
	if err != nil {
		return s.StorageDriver.Move(ctx, source, dest)
	}
	fmt.Printf("\n\tFound layer: %s", layer)
	reader, err := s.Reader(ctx, source, 0)
	if err != nil {
		return err
	}
	defer reader.Close()
	// the url should be in the config
	resp, err := s.pushLayer(layer, reader)
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

func (s *CVMFSStorageMiddleware) Stat(ctx context.Context, path string) (info storagedriver.FileInfo, stgErr error) {
	fmt.Println("stating path:", path)
	info, stgErr = s.StorageDriver.Stat(ctx, path) // storageDriverError => stgErr
	fmt.Println("info:", info, "err:", stgErr)
	if stgErr != nil {
		return
	}
	// if there was an error (file not present) we would have returned already here.
	// from now on, the file is in the docker backend storage

	if info.IsDir() {
		return
	}
	layer, err := getLayerName(path)
	if err != nil {
		// it is not a layer we manage
		return
	}

	fmt.Println("trying get layer info")
	// this should return either 200 or 404
	statusCode := s.checkLayer(layer)
	if statusCode == 200 {
		fmt.Println("layer exists! Good!")
		// the layer exists
		// very happy path
		return
	}

	if statusCode != 404 {
		// it should never happen and we are not really sure what to do
		return
	}
	fmt.Println("layer does not exists")

	// status code == 404
	// the blob is not in the cvmfs storage
	// either it is not while it should be
	// it should not be in, since is a manifest or a config file

	blobType := s.getBlobType(layer)
	fmt.Println("blobType: ", blobType)
	if blobType == manifestBT || blobType == configBT {
		return
	}
	reader, err := s.Reader(ctx, path, 0)
	if err != nil {
		return
	}
	defer reader.Close()
	if blobType == layerBT {
		fmt.Println("trying to push layer: ", path)
		// best / low effort push
		if err != nil {
			return
		}
		resp, err := s.pushLayer(layer, reader)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		return
	}

	// blobType == unknowBT or unrecognizedBT

	gzipCheck := make([]byte, 2)
	if _, err := reader.Read(gzipCheck); err != nil {
		return
	}
	// we are checking the magic number of gzip
	if gzipCheck[0] == 0x1f && gzipCheck[1] == 0x8b {
		// we found a layer!
		// we try to push it
		resp, err := s.pushLayer(layer, reader)
		if err != nil {
			return
		}
		defer resp.Body.Close()
		s.addBlobType(layer, layerBT)
		return
	}
	// it is not a gzip, so it is not a layer
	// let's try to identify it nevertheless
	reader, err = s.Reader(ctx, path, 0)
	if err != nil {
		return
	}
	defer reader.Close()

	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return
	}
	// can it be a manifest?
	manifest := schema2.DeserializedManifest{}
	err = manifest.UnmarshalJSON(bytes)
	if err == nil {
		// found, it is a manifest
		s.addBlobType(layer, manifestBT)
		return
	}

	// can it be a configuration?
	var imageConfig v1.Image
	err = json.Unmarshal(bytes, &imageConfig)
	if err != nil {
		// alright we don't know what it is
		s.addBlobType(layer, unrecognizedBT)
		return
	} else {
		// found it is a configuration file
		s.addBlobType(layer, configBT)
	}

	return
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
