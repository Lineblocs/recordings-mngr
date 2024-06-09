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
	_ "github.com/go-sql-driver/mysql"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

func sendApiRequest(path string, vals map[string]string) (string, error) {
	fullUrl := createApiUrl( path + "?" )

	for k, v := range vals {
		fullUrl = fullUrl + (k + "=" + url.QueryEscape(v)) + "&"
	}
	fmt.Println("URL:>", fullUrl)

	req, err := http.NewRequest("GET", fullUrl, bytes.NewBuffer([]byte("")))
	req.Header.Set("Content-Type", "application/json")

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

	fmt.Println("response Body:", bodyAsString)
	fmt.Println("response Status:", resp.Status)
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

func sendToAssetServer( data []byte, filename string ) (error, string) {
	sess, err := session.NewSession(&aws.Config{
        Region: aws.String(settings.AwsRegion),
        Credentials: credentials.NewStaticCredentials(
			settings.AwsAccessKeyId, 
			settings.AwsSecretAccessKey, 
			""),
    })
	if err != nil {
		return fmt.Errorf("error occured: %v", err), ""
	}


	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	f := bytes.NewReader(data)
	bucket := os.Getenv("S3_BUCKET")
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
	url := createMediaUrl(key)

	return nil, url
}
func processRecordings() (error) {
	fmt.Println("processRecordings called\r\n");
	status := "started"
	results, err:= db.Query("SELECT id, status, storage_id, storage_server_ip, trim FROM recordings WHERE status = ? AND relocation_attempts < 3", status)

	if err != nil {
		return err
	}
  	defer results.Close()
    for results.Next() {
		var id string
		var status string
		var storageId string
		var storageServerIp string
		var trim bool
		err = results.Scan(&id, &status,&storageId,&storageServerIp, &trim)
		if err != nil {
			return err
		}
		fmt.Printf("Storage ID=%s, Server IP=%s\r\n", storageId, storageServerIp)
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
		//data :=[]byte("")
 		//filename := (uniq.String() + ".wav")
 		filename := (storageId+ ".wav")
		// contact the server
		err,link  :=sendToAssetServer(data, filename)
		if err != nil {
			fmt.Println("error:"+err.Error())
			continue
		}
		stmt, err := db.Prepare("UPDATE recordings SET `s3_url` = ?, `status`='processed' WHERE `storage_id` = ?")
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