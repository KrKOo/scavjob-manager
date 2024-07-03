package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/template"
	"time"

	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientConfig "sigs.k8s.io/controller-runtime/pkg/client/config"
)

type Config struct {
	Namespace       string `yaml:"Namespace"`
	JobTemplate     string `yaml:"JobTemplate"`
	JobNamePrefix   string `yaml:"JobNamePrefix"`
	DataDir         string `yaml:"DataDir"`
	RefreshInterval int    `yaml:"RefreshInterval"`
}

func (c *Config) getConfig(configPath string) *Config {
	configFile, err := os.ReadFile(configPath)

	if err != nil {
		log.Printf("Error reading the config file: %v", err)
	}

	err = yaml.Unmarshal(configFile, c)

	if err != nil {
		log.Fatalf("Error parsing the config file: %v", err)
	}

	return c
}

func getK8sClient() client.Client {
	cl, err := client.New(clientConfig.GetConfigOrDie(), client.Options{})

	if err != nil {
		log.Fatalf("Error creating the client: %v", err)
	}

	return cl
}

var ScavengerJobGVK = schema.GroupVersionKind{
	Group:   "core.cerit.cz",
	Version: "v1",
	Kind:    "ScavengerJob",
}

type ScavengerJob struct {
	Name      string
	DataDir   string
	Finished  bool
	Namespace string
}

func (j *ScavengerJob) Get() *unstructured.Unstructured {
	cl := getK8sClient()

	var existingObj unstructured.UnstructuredList

	existingObj.SetGroupVersionKind(ScavengerJobGVK)

	listOptions := client.ListOptions{
		Namespace:     j.Namespace,
		FieldSelector: fields.OneTermEqualSelector("metadata.name", j.Name),
	}

	err := cl.List(context.Background(), &existingObj, &listOptions)

	if err != nil {
		log.Fatalf("Error listing the resource: %v", err)
	}

	if len(existingObj.Items) > 0 {
		return &existingObj.Items[0]
	}

	return nil
}

func (j *ScavengerJob) Run(config Config) bool {
	tpl, err := template.New(j.Name).Parse(config.JobTemplate)

	if err != nil {
		log.Fatalf("Error parsing the job template: %v", err)
	}

	var buf bytes.Buffer
	err = tpl.Execute(&buf, j)

	if err != nil {
		log.Fatalf("Error executing the job template: %v", err)
	}

	decoder := serializer.NewCodecFactory(runtime.NewScheme()).UniversalDeserializer()
	obj := &unstructured.Unstructured{}
	_, _, err = decoder.Decode(buf.Bytes(), nil, obj)
	if err != nil {
		log.Fatalf("Error decoding the job template: %v", err)
	}

	existingObj := j.Get()

	if existingObj != nil {
		log.Println("Job will not be created since it already exists: ", j.Name)
		return true
	}

	cl := getK8sClient()

	err = cl.Create(context.Background(), obj)

	if err != nil {
		log.Printf("Not creating job with workdir %s: %s", j.DataDir, err)
		return false
	}

	log.Println("Created job with workdir: ", j.DataDir)
	return true
}

func (j *ScavengerJob) IsRunning() bool {
	obj := j.Get()
	return obj != nil
}

func (j *ScavengerJob) Delete() {
	cl := getK8sClient()

	obj := j.Get()

	if obj == nil {
		return
	}

	log.Println("Deleting job: ", j.Name)
	err := cl.Delete(context.Background(), obj)

	if err != nil {
		log.Fatalf("Error deleting the resource: %v", err)
	}
}

func deleteJobByName(name string, namespace string) {
	scavJob := ScavengerJob{
		Name:      name,
		DataDir:   "",
		Finished:  true,
		Namespace: namespace,
	}

	scavJob.Delete()
}

func getAllDataDirs(dataDir string) []string {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	var dataDirs []string
	for _, e := range entries {
		// Skip files and hidden directories
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}

		dataDirs = append(dataDirs, e.Name())
	}

	return dataDirs
}

func getAllAvailableJobs(config Config) []ScavengerJob {
	var jobs []ScavengerJob
	dataDirs := getAllDataDirs(config.DataDir)

	for _, dir := range dataDirs {
		dirNameSum := md5.Sum([]byte(dir))
		id := hex.EncodeToString(dirNameSum[:])

		name := fmt.Sprintf("%s-%s", config.JobNamePrefix, id)
		finished := false

		if _, err := os.Stat(filepath.Join(config.DataDir, dir, "finished")); err == nil {
			finished = true
		}

		job := ScavengerJob{
			Name:      name,
			DataDir:   dir,
			Finished:  finished,
			Namespace: config.Namespace,
		}

		jobs = append(jobs, job)
	}

	return jobs
}

func getAllRunningJobs(config Config) []unstructured.Unstructured {
	cl := getK8sClient()

	var existingObjs unstructured.UnstructuredList

	existingObjs.SetGroupVersionKind(ScavengerJobGVK)

	listOptions := client.ListOptions{
		Namespace: config.Namespace,
	}

	err := cl.List(context.Background(), &existingObjs, &listOptions)

	if err != nil {
		log.Fatalf("Error listing the resource: %v", err)
	}

	var jobs []unstructured.Unstructured

	for _, obj := range existingObjs.Items {
		name := obj.GetName()

		if strings.HasPrefix(name, config.JobNamePrefix) {
			jobs = append(jobs, obj)
		}
	}

	return jobs
}

func initJobs(config Config) {
	availableJobs := getAllAvailableJobs(config)

	availableJobNames := []string{}
	for _, job := range availableJobs {
		availableJobNames = append(availableJobNames, job.Name)
	}

	runningJobs := getAllRunningJobs(config)

	for _, job := range availableJobs {
		// Delete any jobs that are finished
		if job.Finished {
			job.Delete()
			continue
		}

		// Run jobs that are not running
		job.Run(config)
	}

	// Delete jobs that are running, but do not have a corresponding data dir anymore
	for _, job := range runningJobs {
		if !slices.Contains(availableJobNames, job.GetName()) {
			deleteJobByName(job.GetName(), config.Namespace)
		}
	}
}

func reconcileLoop(config Config) {
	ticker := time.NewTicker(time.Duration(config.RefreshInterval) * time.Second)
	var oldJobIds []string

	for range ticker.C {
		newJobs := getAllAvailableJobs(config)

		var newJobIds []string
		for _, newJob := range newJobs {
			if newJob.Finished {
				// Delete finished jobs
				if slices.Contains(oldJobIds, newJob.Name) {
					log.Println("Deleting finished job with workdir: ", newJob.DataDir)
					newJob.Delete()
				}
				continue
			}

			// Skip jobs that are already running
			if slices.Contains(oldJobIds, newJob.Name) {
				newJobIds = append(newJobIds, newJob.Name)
				continue
			}

			started := newJob.Run(config)

			if !started {
				continue
			}

			newJobIds = append(newJobIds, newJob.Name)
		}

		for _, oldJobId := range oldJobIds {
			if !slices.Contains(newJobIds, oldJobId) {
				log.Println("Deleting job with id: ", oldJobId)
				deleteJobByName(oldJobId, config.Namespace)
			}
		}

		oldJobIds = newJobIds
	}
}

func main() {
	configFile := flag.String("config", "", "Path to the configuration file")

	flag.Parse()

	if *configFile == "" {
		log.Println("Error: config argument is required")
		flag.Usage()
		os.Exit(1)
	}

	config := Config{}
	config.getConfig(*configFile)

	initJobs(config)

	reconcileLoop(config)
}
