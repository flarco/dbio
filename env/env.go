package env

import (
	"net/url"
	"os"
)

var envVars = []string{
	"DBIO_PARALLEL", "AWS_BUCKET", "AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_ENDPOINT", "AWS_REGION",
	"COMPRESSION", "FILE_MAX_ROWS", "DBIO_SAMPLE_SIZE",
	"GC_BUCKET", "GC_CRED_FILE", "GSHEETS_CRED_FILE",
	"GC_CRED_JSON_BODY", "GC_CRED_JSON_BODY_ENC", "GC_CRED_API_KEY",
	"AZURE_ACCOUNT", "AZURE_KEY", "AZURE_CONTAINER", "AZURE_SAS_SVC_URL",
	"AZURE_CONN_STR", "SSH_TUNNEL", "SSH_PRIVATE_KEY", "SSH_PUBLIC_KEY",
	"DBIO_CONCURENCY_LIMIT",

	"DBIO_SMTP_HOST", "DBIO_SMTP_PORT", "DBIO_SMTP_USERNAME", "DBIO_SMTP_PASSWORD", "DBIO_SMTP_FROM_EMAIL", "DBIO_SMTP_REPLY_EMAIL",

	"SFTP_USER", "SFTP_PASSWORD", "SFTP_HOST", "SFTP_PORT",
	"SSH_PRIVATE_KEY", "SFTP_PRIVATE_KEY", "SFTP_URL",

	"HTTP_USER", "HTTP_PASSWORD", "GSHEET_CLIENT_JSON_BODY",
	"GSHEET_SHEET_NAME", "GSHEET_MODE",

	"DIGITALOCEAN_ACCESS_TOKEN", "GITHUB_ACCESS_TOKEN",
	"SURVEYMONKEY_ACCESS_TOKEN",

	"DBIO_SEND_ANON_USAGE", "DBIO_HOME",
}

// Vars are the variables we are using
func Vars() (vars map[string]string) {
	vars = map[string]string{}
	// get default from environment
	for _, k := range envVars {
		if vars[k] == "" {
			vars[k] = os.Getenv(k)
		}
	}

	// default as true
	for _, k := range []string{} {
		if vars[k] == "" {
			vars[k] = "true"
		}
	}

	if vars["DBIO_CONCURENCY_LIMIT"] == "" {
		vars["DBIO_CONCURENCY_LIMIT"] = "10"
	}

	if vars["DBIO_SAMPLE_SIZE"] == "" {
		vars["DBIO_SAMPLE_SIZE"] = "900"
	}

	if bodyEnc := vars["GC_CRED_JSON_BODY_ENC"]; bodyEnc != "" {
		body, _ := url.QueryUnescape(bodyEnc)
		vars["GC_CRED_JSON_BODY"] = body
	}
	return
}
