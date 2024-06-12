package main
import (
	//"log"
	"io/ioutil"
	"os"
	"fmt"
	"database/sql"
	"context"
	"time"
	"bytes"

	"net/url"
	"net/http"
	"encoding/json"
	"errors"
	"strconv"
	_ "github.com/go-sql-driver/mysql"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
    "github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/joho/godotenv"
	_ "github.com/google/uuid"
	lineblocs "github.com/Lineblocs/go-helpers"
	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/native"
)

type Settings struct {
	AwsAccessKeyId           string `json:"aws_access_key_id"`
	AwsSecretAccessKey       string `json:"aws_secret_access_key"`
	AwsRegion                string `json:"aws_region"`
	S3Bucket               string `json:"s3_bucket"`
	StripePubKey             string `json:"stripe_pub_key"`
	StripePrivateKey         string `json:"stripe_private_key"`
	StripeTestPubKey         string `json:"stripe_test_pub_key"`
	StripeTestPrivateKey     string `json:"stripe_test_private_key"`
	StripeMode               string `json:"stripe_mode"`
	SmtpHost                 string `json:"smtp_host"`
	SmtpPort                 string `json:"smtp_port"`
	SmtpUser                 string `json:"smtp_user"`
	SmtpPassword             string `json:"smtp_password"`
	SmtpTls                  string `json:"smtp_tls"`
	GoogleServiceAccountJson string `json:"google_service_account_json"`
}

var db* sql.DB;
var settings* Settings;


func trimSilence( data []byte ) ([]byte, error) {

	return data, nil

}

func getLineblocsKey( ) (string) {
	var key string = os.Getenv("LINEBLOCS_KEY")
	return key
}

func createMediaUrl(s3Key string) (string) {
	baseUrl := os.Getenv("MEDIA_API_URL")
	return (baseUrl + "/" + s3Key)
}

func createApiUrl( path string ) (string) {
	//var baseUrl string = "https://internals." + os.Getenv("DEPLOYMENT_DOMAIN")
	var baseUrl string = os.Getenv("API_URL")
	return baseUrl + path
}

func createARIConnection(connectCtx context.Context, serverIp string) (ari.Client, error) {
	fmt.Println("Connecting to: " + os.Getenv("ARI_URL"))
	ariApp:=os.Getenv("ARI_RECORDING_APP")
	url:= os.Getenv("ARI_URL")
	wsUrl:= os.Getenv("ARI_WSURL")
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

	fmt.Println("Connected to ARI server successfully.")
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

func sendApiRequest(path string, vals map[string]string) (string, error) {
	fullUrl := createApiUrl( path + "?" )

	for k, v := range vals {
		fullUrl = fullUrl + (k + "=" + url.QueryEscape(v)) + "&"
	}
	fmt.Println("URL:>", fullUrl)

	req, err := http.NewRequest("GET", fullUrl, bytes.NewBuffer([]byte("")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Lineblocs-Api-Token", getLineblocsKey())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyAsString := string(body)

	status := resp.StatusCode
	if !(status >= 200 && status <= 299) {
		return "", errors.New("Status: " + resp.Status + " result: " + bodyAsString)
	}
	return bodyAsString, nil
}

func getSettings() (*Settings, error) {
	fmt.Println("getting settings")

	params := make(map[string]string)
	res, err := sendApiRequest("/user/getSettings", params)
	if err != nil {
		return nil, err
	}

	var data Settings
	err = json.Unmarshal([]byte(res), &data)
	if err != nil {
		fmt.Println("get settings err " + err.Error())
		return nil, err
	}

	return &data, nil
}

func sendToAssetServer(data []byte, filename string) (string, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(settings.AwsRegion),
		Credentials: credentials.NewStaticCredentials(
			settings.AwsAccessKeyId,
			settings.AwsSecretAccessKey,
			"",
		),
	})
	if err != nil {
		return "", fmt.Errorf("error occurred while creating AWS session: %v", err)
	}

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	f := bytes.NewReader(data)
	//bucket := os.Getenv("S3_BUCKET")
	bucket := settings.S3Bucket
	if bucket == "" {
		return "", fmt.Errorf("S3_BUCKET environment variable is not set")
	}
	key := "recordings/" + filename

	fmt.Printf("Uploading to %s\n", key)
	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload file, %v", err)
	}

	s3Url := aws.StringValue(&result.Location)

	fmt.Printf("file uploaded to, %s\n", s3Url)

	// send back link to media
	// url := createMediaUrl(key)

	// return url, nil

	return s3Url, nil
}

func processRecordings() (error) {
	fmt.Println("processRecordings called\r\n");
	status := "completed"
	results, err:= db.Query("SELECT id, status, storage_id, storage_server_ip, trim FROM recordings WHERE status = ?", status)

	if err != nil {
		return err
	}
  	defer results.Close()
    for results.Next() {
		var id int
		var status string
		var storageId string
		var storageServerIp string
		var trim bool

		fmt.Println("processRecordings processing record: " + strconv.Itoa(id))
		err = results.Scan(&id, &status,&storageId,&storageServerIp, &trim)
		if err != nil {
			fmt.Println("error:"+err.Error())
			continue
		}

		if storageId == "" {
			fmt.Println("storage id is empty for recording id: " + strconv.Itoa(id))
			continue
		}

		fmt.Printf("Storage ID=%s, Server IP=%s\r\n", storageId, storageServerIp)
		ctx :=context.Background()
		client, err := createARIConnection(ctx, storageServerIp)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		src := ari.NewKey(ari.StoredRecordingKey, strconv.Itoa(id))
		data,err := client.StoredRecording().File(src)

		if err != nil {
			fmt.Println(err.Error())
			stmt, err := db.Prepare("UPDATE recordings SET `relocation_attempts` = relocation_attempts + 1 WHERE `storage_id` = ?")
			if err != nil {
				fmt.Println("error:"+err.Error())
				continue
			}
			defer stmt.Close()
			_, err = stmt.Exec(storageId)
			if err != nil {
				fmt.Println("error:"+err.Error())
				continue
			}
		}
		if trim {
			data,err = trimSilence( data )
			if err != nil {
				fmt.Println("error:"+err.Error())
				continue
			}
		}

		fmt.Printf("sending recording %d to asset server", id)
		//data :=[]byte("")
 		//filename := (uniq.String() + ".wav")
 		filename := (storageId+ ".wav")
		// contact the server
		link,err :=sendToAssetServer(data, filename)
		if err != nil {
			fmt.Println("error:"+err.Error())
			continue
		}

		fmt.Printf("generated S3 link: %s", link)
		stmt, err := db.Prepare("UPDATE recordings SET `s3_url` = ?, `status`='processed' WHERE `storage_id` = ?")
		if err != nil {
			fmt.Println("error while db:"+err.Error())
			continue
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

	loadDotEnv := os.Getenv("USE_DOTENV")
	if loadDotEnv != "off" {
		fmt.Println("loading env settings with dotenv")
		err := godotenv.Load(".env")
		if err != nil {
			panic(err)
		}
	}

	settings, err = getSettings()
	if err != nil {
		panic(err)
	}

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