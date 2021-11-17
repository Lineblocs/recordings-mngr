package main
import (
	//"log"
	"io/ioutil"
	"os"
	"fmt"
	"database/sql"
	"context"
	"time"
	_ "github.com/go-sql-driver/mysql"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	lineblocs "bitbucket.org/infinitet3ch/lineblocs-go-helpers"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/native"
)

var db* sql.DB;


func createARIConnection(connectCtx context.Context, serverIp string) (ari.Client, error) {
 	fmt.Println("Connecting to: " + os.Getenv("ARI_URL"))
	 ariApp:="lineblocs-recordings"
	 url:= "http://" + serverIp + ":8088/ari"
	 wsUrl:= "ws://" + serverIp + ":8088/ari/events"
       cl, err := native.Connect(&native.Options{
               Application:  ariApp,
               Username:     os.Getenv("ARI_USERNAME"),
               Password:     os.Getenv("ARI_PASSWORD"),
               URL:          url,
               WebsocketURL: wsUrl})
        if err != nil {
               fmt.Println("Failed to build native ARI client", "error", err)
               fmt.Println( "error occured: " + err.Error() )
               return nil, err
        }
       return cl, err
 }

func createTemporaryFile(data []byte, filename string) (string, error) {
	var folder string = "/tmp/"
	fullPathToFile := folder + filename
	err := ioutil.WriteFile(fullPathToFile, data, 0644)
	if err != nil {
			fmt.Println(err.Error())
			return "", err
	}
	return fullPathToFile, nil
}
func sendToAssetServer( path string, filename string ) (error, string) {
	sess, err := session.NewSession(&aws.Config{ Region: aws.String(os.Getenv("AWS_DEFAULT_REGION")) })
	if err != nil {
		return fmt.Errorf("error occured: %v", err), ""
	}


	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	f, err  := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", path, err), ""
	}

	bucket := "lineblocs"
	key := "recordings/" + filename

	fmt.Printf("Uploading to %s\r\n", key)
	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err), ""
	}
	fmt.Printf("file uploaded to, %s\n", aws.StringValue(&result.Location))


	// send back link to media
	url := "https://lineblocs.s3.ca-central-1.amazonaws.com/" + key
	return nil, url
}
func processRecordings() (error) {
	fmt.Println("processRecordings called\r\n");
	status := "pending"
	results, err:= db.Query("SELECT id, status, storage_id, storage_server_ip FROM recordings WHERE status = ?", status)

	if err != nil {
		return err
	}
  	defer results.Close()
    for results.Next() {
		var id string
		var status string
		var storageId string
		var storageServerIp string
		err = results.Scan(&id, &status,&storageId,&storageServerIp)
		if err != nil {
			return err
		}
		fmt.Printf("Storage ID=%s, Server IP=%s\r\n", storageId, storageServerIp)
		uniq, err := uuid.NewUUID()
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		ctx :=context.Background()
		client, err := createARIConnection(ctx, storageServerIp)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		src := ari.NewKey(ari.StoredRecordingKey, storageId)
		//data,err := client.StoredRecording().File(src)
		data,err := client.StoredRecording().File(src)
		if err != nil {
			fmt.Println(err.Error())
			stmt, err := db.Prepare("UPDATE recordings SET `relocation_attempts` = relocation_attempts + 1 WHERE `storage_id` = ?")
			if err != nil {
				return err
			}
			defer stmt.Close()
			_, err = stmt.Exec(storageId)
			if err != nil {
				return err
			}
			return err
		}
		//data :=[]byte("")
 		filename := (uniq.String() + ".wav")
		// contact the server
 		path, err :=createTemporaryFile(data, filename)
		if err != nil {
			return err
		}
		err,link  :=sendToAssetServer(path, filename)
		if err != nil {
			return err
		}
		stmt, err := db.Prepare("UPDATE recordings SET `s3_url` = ? WHERE `storage_id` = ?")
		if err != nil {
			return err
		}
		defer stmt.Close()
		_, err = stmt.Exec(link, storageId)
		if err != nil {
			return err
		}
	}
	return nil
}
func main() {
	var err error
	db, err =lineblocs.CreateDBConn()
	if err != nil {
		panic(err)
	}
	for ;; {
		err := processRecordings()
		if err != nil {
			fmt.Println("error:"+ err.Error())
		}
		time.Sleep(time.Duration(time.Second * 5))
	}
}