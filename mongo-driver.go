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
	"sync"
	"time"
)

var (
	driverInstances = sync.Map{}
	mutex           = sync.Mutex{}
)

type MongoDriver struct {
	uri       string
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

	MaxPoolSize     uint64        // default 100
	MinPoolSize     uint64        // default 0
	MaxConnIdleTime time.Duration // default 0
	ConnectTimeout  time.Duration // default 30s
}

func (opts MongoDriverOptions) standardize() MongoDriverOptions {
	if opts.MaxPoolSize == 0 {
		opts.MaxPoolSize = 100
	}
	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = 30 * time.Second
	}

	return opts
}

func (opts MongoDriverOptions) URI() string {
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
	return fmt.Sprintf("mongodb://%s:%s@%s/%s%s", opts.Username, opts.Password, endpointPart, opts.Database, authSource)
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

// NewMongoDriver 创建一个新的MongoDriver实例，根据连接信息判断已经创建过的实例是否存在，如果存在则返回已存在的实例，否则创建新的实例。
func NewMongoDriver(opts MongoDriverOptions) (*MongoDriver, error) {
	mutex.Lock()
	defer mutex.Unlock()

	opts = opts.standardize()
	uri := opts.URI()

	var (
		driver *MongoDriver
		client *mongo.Client
		err    error
	)
	if d, ok := driverInstances.Load(uri); ok {
		driver = d.(*MongoDriver)
	} else {
		client, err = connect(opts)
		if err != nil {
			return nil, err
		}
		driver = &MongoDriver{
			uri:       uri,
			client:    client,
			db:        client.Database(opts.Database),
			fsBuckets: make(map[string]*gridfs.Bucket),
		}
		driverInstances.Store(uri, driver)
	}
	return driver, nil
}

// CloseAllMongoDrivers 关闭所有MongoDriver实例，用于程序退出时关闭所有连接。
// 注意：此方法会关闭所有MongoDriver实例，包括已经创建的实例和新创建的实例。
func CloseAllMongoDrivers() {
	driverInstances.Range(func(key, value interface{}) bool {
		driver := value.(*MongoDriver)
		_ = driver.Close()
		return true
	})
}

// GetCollection 获取指定名称的集合。
func (d *MongoDriver) GetCollection(name string) *mongo.Collection {
	return d.db.Collection(name)
}

// GetGridfsBucket 获取指定名称的GridFS桶。
// 如果名称为空或为默认名称，则返回默认桶。
// 如果名称不为空，则返回指定名称的桶。
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

// FileExists 判断指定名称的文件是否存在。
// 如果文件存在，则返回true，否则返回false。
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

// UploadFile 上传文件到指定名称的GridFS桶。
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

// DownloadFile 下载指定名称的文件。
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

// GetFileDownloadStream 获取指定名称的文件下载流。
func (d *MongoDriver) GetFileDownloadStream(gridfsBucketName, fileID string) (stream *gridfs.DownloadStream, err error) {
	var bucket *gridfs.Bucket
	bucket, err = d.GetGridfsBucket(gridfsBucketName)
	if err != nil {
		return
	}
	stream, err = bucket.OpenDownloadStream(fileID)
	return
}

// DeleteFile 删除指定名称的文件。
func (d *MongoDriver) DeleteFile(gridfsBucketName, fileID string) error {
	bucket, err := d.GetGridfsBucket(gridfsBucketName)
	if err != nil {
		return err
	}
	err = bucket.Delete(fileID)
	if err != nil && !errors.Is(err, gridfs.ErrFileNotFound) {
		return err
	}
	return nil
}

// Close 关闭MongoDriver实例。
func (d *MongoDriver) Close() error {
	driverInstances.Delete(d.uri)
	return d.client.Disconnect(context.Background())
}

func connect(opts MongoDriverOptions) (*mongo.Client, error) {
	connOpts := options.Client().ApplyURI(opts.URI())
	connOpts.SetMaxPoolSize(opts.MinPoolSize)
	connOpts.SetMinPoolSize(opts.MinPoolSize)
	connOpts.SetMaxConnIdleTime(opts.MaxConnIdleTime)
	connOpts.SetConnectTimeout(opts.ConnectTimeout)

	client, err := mongo.Connect(context.Background(), connOpts)
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

// CreateIndex 创建索引，如果索引已经存在，则不创建。
// 如果索引不存在，则创建索引。
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

// RemoveIndex 删除指定名称的索引。
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

// RemoveIndexByOption 删除指定选项的索引。
// 注意：此方法会根据IndexOption的Keys生成索引名称，然后删除指定名称的索引。
func RemoveIndexByOption(c *mongo.Collection, opts ...*IndexOption) error {
	var indexNames []string
	for _, opt := range opts {
		generateIndexName(opt)
		indexNames = append(indexNames, opt.Name)
	}

	return RemoveIndex(c, indexNames...)
}

// CursorList 获取指定选项的游标。用于分页查询。 返回游标可以更灵活地处理查询结果。
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

// List 获取指定选项的查询结果。用于分页查询。 直接返回查询结果，不需要手动处理游标。
func List(c *mongo.Collection, opt *ListOption, results interface{}) error {
	cursor, err := CursorList(c, opt)
	if err != nil {
		return err
	}

	return cursor.All(context.Background(), results)
}
