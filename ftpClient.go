package bloodlabnet

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/pkg/sftp"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

type FTPType int

const FTP FTPType = 1
const SFTP FTPType = 2

type AuthenticationMethod int

const PASSWORD AuthenticationMethod = 1
const PUBKEY AuthenticationMethod = 2

type FTPConfiguration interface {
	UserPass(user, pass string)
	HostKey(hostkey string)
	PubKey(user, pubkey string)
	PollInterval(pollInterval time.Duration)
}

type ftpConfiguration struct {
	authMethod      AuthenticationMethod
	user            string
	key             string
	hostkey         string
	pollInterval    time.Duration
	deleteAfterRead bool
	dialTimeout     time.Duration
}

type ftpServerInstance struct {
	ftpType  FTPType
	host     string
	port     int
	hostpath string
	filemask string
	config   *ftpConfiguration
}

func CreateFTP(ftptype FTPType, host string, port int, hostpath, filemask string, config *ftpConfiguration) (ConnectionInstance, error) {
	instance := &ftpServerInstance{
		ftpType:  ftptype,
		host:     host,
		port:     port,
		config:   config,
		hostpath: hostpath,
		filemask: filemask,
	}
	return instance, nil
}

func (conf *ftpConfiguration) UserPass(user, pass string) *ftpConfiguration {
	conf.user = user
	conf.key = pass
	conf.authMethod = PASSWORD
	return conf
}

func (conf *ftpConfiguration) HostKey(hostkey string) *ftpConfiguration {
	conf.hostkey = hostkey
	return conf
}

func (conf *ftpConfiguration) PubKey(user, pubkey string) *ftpConfiguration {
	conf.user = user
	conf.key = pubkey
	conf.authMethod = PUBKEY
	return conf
}

func (conf *ftpConfiguration) PollInterval(pollInterval time.Duration) *ftpConfiguration {
	conf.pollInterval = pollInterval
	return conf
}

func (conf *ftpConfiguration) DeleteAfterRead() *ftpConfiguration {
	conf.deleteAfterRead = true
	return conf
}

func (conf *ftpConfiguration) DontDeleteAfterRead() *ftpConfiguration {
	conf.deleteAfterRead = false
	return conf
}

func (conf *ftpConfiguration) DialTimeout(dialTimeout time.Duration) *ftpConfiguration {
	conf.dialTimeout = dialTimeout
	return conf
}

func DefaultFTPConfig() *ftpConfiguration {
	return &ftpConfiguration{
		authMethod:      PASSWORD,
		pollInterval:    60 * time.Second,
		deleteAfterRead: true,
		dialTimeout:     10 * time.Second,
	}
}

func (instance *ftpServerInstance) Run(handler Handler) {
	switch instance.ftpType {
	case FTP:
		instance.runWithFTP(handler)
	case SFTP:
		instance.runWithSFTP(handler)
	default:
		//TODO handler.Error()
	}
}

func (instance *ftpServerInstance) Stop() {

}

func (instance *ftpServerInstance) FindSessionsByIp(ip string) []Session {
	return []Session{}
}

func (instance *ftpServerInstance) runWithSFTP(handler Handler) {

	sshConfig := &ssh.ClientConfig{}

	switch instance.config.authMethod {
	case PASSWORD:
		sshConfig.User = instance.config.user
		sshConfig.Auth = []ssh.AuthMethod{
			ssh.Password(instance.config.key),
		}
	case PUBKEY:
		// TODO not implemented
	default:
		// this should never happen :)
		log.Panic().Msg("Invalid (s)FTP Authentication-Method provided")
	}

	if instance.config.hostkey != "" {
		base64Key := []byte(instance.config.hostkey)
		key := make([]byte, base64.StdEncoding.DecodedLen(len(base64Key)))
		n, err := base64.StdEncoding.Decode(key, base64Key)
		if err != nil {
			//TODO: error
			return
		}
		key = key[:n]
		hostKey, err := ssh.ParsePublicKey(key)
		if err != nil {
			//TODO: error
			return
		}
		sshConfig.HostKeyCallback = ssh.FixedHostKey(hostKey)
	} else {
		sshConfig.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	}

	sshConfig.Timeout = instance.config.dialTimeout

	ftpserver := fmt.Sprintf("%s:%d", instance.host, instance.port)

	sshConnection, err := ssh.Dial("tcp", ftpserver, sshConfig)
	if err != nil {
		log.Error().Err(err).Msg("Open - Dail")
		// TODO LOG Err
		return
	}

	sftpClient, err := sftp.NewClient(sshConnection)
	if err != nil {
		log.Error().Err(err).Msg("Open - NewClient")
		sshConnection.Close()
		// TODO LOG ERROR
		return
	}

	if handler.Connected(instance) != nil {
		// TODO error handler
		return
	}

	ftpPath := strings.TrimRight(instance.hostpath, "/") + "/"

	for {

		time.Sleep(instance.config.pollInterval)

		files, err := sftpClient.ReadDir(ftpPath)
		if err != nil {
			log.Error().Err(err).Msgf("ReadFiles - ReadDir %s", ftpPath)
			// TODO ERROR
			// return orderFiles
		}

		if len(files) == 0 {
			continue
		}

		for _, file := range files {
			match, err := sftp.Match(strings.ToUpper(instance.filemask), strings.ToUpper(file.Name()))
			if err != nil {
				log.Error().Err(err).Msg("ReadFiles - Match")
				continue
			}
			if match {
				log.Info().Msgf("readsFtpOrders filename: %s ModTime: %+v", file.Name(), file.ModTime())

				filename := fmt.Sprintf("%s%s", ftpPath, file.Name())

				// Rename the file to "in process" before opening it
				filename_processing := filename + ".processing"
				err = sftpClient.Rename(filename, filename_processing)
				if err != nil {
					log.Error().Err(err).Msg(fmt.Sprintf("Skipping file %s", filename_processing))
				}

				// open file and read content
				fileread, err := sftpClient.Open(filename + ".processing")
				if err != nil {
					log.Error().Err(err).Msg("ReadFiles - Open")
					continue
				}
				filebuffer, err := ioutil.ReadAll(fileread)
				if err != nil {
					log.Error().Err(err).Msg("ReadFiles - ReadAll")
					continue
				}

				err = fileread.Close()
				if err != nil {
					log.Error().Err(err).Msg("ReadFiles - Close")
					continue
				}

				err = handler.DataReceived(instance, filebuffer, file.ModTime())

				if err == nil && instance.config.deleteAfterRead { // if processing was successful
					err := sftpClient.Remove(filename + ".processing")
					if err != nil {
						log.Error().Err(err).Msg(filename + ".processing")
						// TODO Errorhandling
					}
				}

			}
		}
	}

	return
}

func (instance *ftpServerInstance) runWithFTP(handler Handler) {

	var err error

	ftpClient, err := ftp.Dial(fmt.Sprintf("%s:%d", instance.host, instance.port), ftp.DialWithTimeout(instance.config.dialTimeout))
	if err != nil {
		log.Error().Err(err).Msg("Open - Dial")
		// TODO Error handling
		return
	}

	if instance.config.authMethod != PASSWORD {
		//TODO: Error handling invalid authentication
	}

	err = ftpClient.Login(instance.config.user, instance.config.key)
	if err != nil {
		log.Error().Err(err).Msg("Open - Login")
		// TODO Error handling
		return
	}

	if handler.Connected(instance) != nil {
		// TODO error handler
		return
	}

	ftpPath := strings.TrimRight(instance.hostpath, "/") + "/"

	for {

		time.Sleep(instance.config.pollInterval)

		files, err := ftpClient.List(ftpPath)
		if err != nil {
			log.Error().Err(err).Msg("ReadFiles - List")
			// TODO Error handling
			continue
		}

		for _, file := range files {

			match, err := sftp.Match(strings.ToUpper(instance.filemask), strings.ToUpper(file.Name))
			if err != nil {
				log.Error().Err(err).Msg("ReadFiles - Match")
				continue
			}

			if match {
				log.Info().Msgf("readsFtpOrders filename: %s", file.Name)

				// Rename to .processing
				err := ftpClient.Rename(ftpPath+file.Name, ftpPath+file.Name+".processing")
				if err != nil {
					log.Error().Err(err).Msg("RenameFile")
					continue
				}

				// Read the content
				fileread, err := ftpClient.Retr(ftpPath + file.Name + ".processing")
				if err != nil {
					log.Error().Err(err).Msg("ReadFiles - Retr")
					continue
				}
				filebuffer, err := ioutil.ReadAll(fileread)
				if err != nil {
					log.Error().Err(err).Msg("ReadFiles - ReadAll")
					continue
				}

				err = fileread.Close()
				if err != nil {
					log.Error().Err(err).Msg("ReadFiles - Close")
					continue
				}

				err = handler.DataReceived(instance, filebuffer, file.Time)

				if err == nil && instance.config.deleteAfterRead { // if processing was successful
					err := ftpClient.Delete(ftpPath + file.Name + ".processing")
					if err != nil {
						log.Error().Err(err).Msg(ftpPath + file.Name + ".processing")
						// TODO Error handling
					}
				}
			}
		}
	}
}

func (instance *ftpServerInstance) IsAlive() bool {
	return true // TODO
}

func (instance *ftpServerInstance) Send(msg [][]byte) (int, error) {
	return 0, errors.New("Not implemented")

}

func (instance *ftpServerInstance) Receive() ([]byte, error) {
	return []byte{}, errors.New("Not implemented")
}

func (instance *ftpServerInstance) Close() error {
	return errors.New("Not implemented")
}

func (instance *ftpServerInstance) WaitTermination() error {
	return errors.New("Not implemented")
}

func (instance *ftpServerInstance) RemoteAddress() (string, error) {
	return instance.host, nil
}
