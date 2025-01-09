package mongo_driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sort"
	"strings"
	"time"
)

type MongoDriver struct {
	client    *mongo.Client
	db        *mongo.Database
	fsBuckets map[string]*gridfs.Bucket
}

type MongoDriverOptions struct {
	Endpoints  []string
	Database   string
	Host       string
	Port       int
	Username   string
	Password   string
	AuthSource string
}

type IndexOption struct {
	Name string
	//Keys   map[string]interface{}
	Keys   bson.D
	Unique bool
}

type ListOption struct {
	Filter     interface{}
	Sorter     interface{}
	Projection interface{}
	Limit      int64
	Skip       int64
}

func NewMongoDriver(opts MongoDriverOptions) (*MongoDriver, error) {
	authSource := ""
	if opts.AuthSource != "" {
		authSource = fmt.Sprintf("?authSource=%s", opts.AuthSource)
	}

	endpointPart := ""
	if opts.Endpoints != nil && len(opts.Endpoints) > 0 {
		endpointPart = fmt.Sprintf("%s", strings.Join(opts.Endpoints, ","))
	} else {
		endpointPart = fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	}

	fmt.Println("[INFO]", "Connecting to MongoDB", fmt.Sprintf("mongodb://%s:******@%s/%s%s", opts.Username, endpointPart, opts.Database, authSource))
	client, err := connect(fmt.Sprintf("mongodb://%s:%s@%s/%s%s", opts.Username, opts.Password, endpointPart, opts.Database, authSource))
	if err != nil {
		return nil, err
	}

	return &MongoDriver{
		client:    client,
		db:        client.Database(opts.Database),
		fsBuckets: make(map[string]*gridfs.Bucket),
	}, nil
}

func (d *MongoDriver) GetCollection(name string) *mongo.Collection {
	return d.db.Collection(name)
}

func (d *MongoDriver) GetGridfsBucket(name string) (bucket *gridfs.Bucket, err error) {
	var ok bool
	bucket, ok = d.fsBuckets[name]
	if !ok {
		if name == "" || name == options.DefaultName {
			bucket, err = gridfs.NewBucket(d.db)
		} else {
			opts := options.GridFSBucket().SetName(name)
			bucket, err = gridfs.NewBucket(d.db, opts)
		}
		d.fsBuckets[name] = bucket
	}
	return
}

func (d *MongoDriver) FileExists(bucketName, fileID string) (bool, error) {
	filesCollection := d.GetCollection(bucketName + ".files")

	_, err := filesCollection.FindOne(context.Background(), bson.M{"_id": fileID}).DecodeBytes()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (d *MongoDriver) UploadFile(gridfsBucketName, fileID, fileName string, fileContent []byte) error {
	bucket, err := d.GetGridfsBucket(gridfsBucketName)
	if err != nil {
		return err
	}
	err = bucket.UploadFromStreamWithID(fileID, fileName, bytes.NewBuffer(fileContent))
	if err != nil {
		return err
	}
	return nil
}

func (d *MongoDriver) DownloadFile(gridfsBucketName, fileID string) (fileInfo *gridfs.File, fileContent []byte, err error) {
	var stream *gridfs.DownloadStream
	stream, err = d.GetFileDownloadStream(gridfsBucketName, fileID)
	if err != nil {
		return
	}
	defer func() {
		err := stream.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	fileInfo = stream.GetFile()
	var bucket *gridfs.Bucket
	bucket, err = d.GetGridfsBucket(gridfsBucketName)
	if err != nil {
		return
	}
	fileBuffer := bytes.NewBuffer(nil)
	_, err = bucket.DownloadToStream(fileID, fileBuffer)
	if err != nil {
		return
	}
	fileContent = fileBuffer.Bytes()
	return
}

func (d *MongoDriver) GetFileDownloadStream(gridfsBucketName, fileID string) (stream *gridfs.DownloadStream, err error) {
	var bucket *gridfs.Bucket
	bucket, err = d.GetGridfsBucket(gridfsBucketName)
	if err != nil {
		return
	}
	stream, err = bucket.OpenDownloadStream(fileID)
	return
}

func (d *MongoDriver) DeleteFile(gridfsBucketName, fileID string) error {
	bucket, err := d.GetGridfsBucket(gridfsBucketName)
	if err != nil {
		return err
	}
	err = bucket.Delete(fileID)
	if err != nil && err != gridfs.ErrFileNotFound {
		return err
	}
	return nil
}

func connect(mongoUri string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(mongoUri)

	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getCollectionIndexes(c *mongo.Collection) ([]bson.M, error) {
	iv := c.Indexes()
	cursor, err := iv.List(context.Background(), options.ListIndexes().SetMaxTime(2*time.Second))
	if err != nil {
		return nil, err
	}
	var results []bson.M
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func hasIndex(c *mongo.Collection, idxName string) (bool, error) {
	r, err := getCollectionIndexes(c)
	if err != nil {
		return false, err
	}

	for _, idx := range r {
		if idx["name"] == idxName {
			return true, nil
		}
	}

	return false, nil
}

func generateIndexName(opts *IndexOption) {
	if opts.Name == "" {
		var fields []string
		for _, item := range opts.Keys {
			fields = append(fields, fmt.Sprintf("%s_%v", item.Key, item.Value))
		}
		sort.Strings(fields)
		opts.Name = fmt.Sprintf("idx_%s", strings.Join(fields, "_"))
	}
}

func CreateIndex(c *mongo.Collection, opts ...*IndexOption) error {
	for _, opt := range opts {
		generateIndexName(opt)
		exists, err := hasIndex(c, opt.Name)
		log.Println(opt.Name, exists)
		if err != nil {
			return err
		}
		if !exists {
			opts := options.Index()
			opts.SetUnique(opt.Unique).SetName(opt.Name)
			im := mongo.IndexModel{
				Keys:    opt.Keys,
				Options: opts,
			}
			iv := c.Indexes()
			str, err := iv.CreateOne(context.Background(), im)
			log.Println(str)
			log.Println(err)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func RemoveIndex(c *mongo.Collection, indexNames ...string) error {
	iv := c.Indexes()
	for _, indexName := range indexNames {
		_, err := iv.DropOne(context.Background(), indexName)
		if err != nil {
			return err
		}
	}
	return nil
}

func RemoveIndexByOption(c *mongo.Collection, opts ...*IndexOption) error {
	var indexNames []string
	for _, opt := range opts {
		generateIndexName(opt)
		indexNames = append(indexNames, opt.Name)
	}

	return RemoveIndex(c, indexNames...)
}

func CursorList(c *mongo.Collection, opt *ListOption) (cursor *mongo.Cursor, err error) {
	opts := options.Find()
	if opt.Limit > 0 {
		opts.SetLimit(opt.Limit).SetSkip(opt.Skip)
	}
	if opt.Sorter != nil {
		opts.SetSort(opt.Sorter)
	}

	if opt.Filter == nil {
		opt.Filter = bson.D{{}}
	}

	if opt.Projection != nil {
		opts.SetProjection(opt.Projection)
	}

	return c.Find(context.Background(), opt.Filter, opts)
}

func List(c *mongo.Collection, opt *ListOption, results interface{}) error {
	cursor, err := CursorList(c, opt)
	if err != nil {
		return err
	}

	return cursor.All(context.Background(), results)
}
