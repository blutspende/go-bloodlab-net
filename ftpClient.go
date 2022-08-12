package bloodlabnet

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
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

type LINEBREAK int

const LINEBREAK_CR LINEBREAK = 0x0d
const LINEBREAK_CRLF LINEBREAK = 0x0d0a
const LINEBREAK_LF LINEBREAK = 0x0a

type ftpConfiguration struct {
	authMethod             AuthenticationMethod
	user                   string
	key                    string
	hostkey                string
	pollInterval           time.Duration
	deleteAfterRead        bool
	dialTimeout            time.Duration
	linebreak              LINEBREAK
	autoGeneratedFilenames struct {
		generatorpattern string
		prefix           string
		suffix           string
		counter          int
	}
}

type ftpServerInstance struct {
	ftpType            FTPType
	host               string
	port               int
	hostpath           string
	filemask           string
	config             *ftpConfiguration
	isRunning          bool
	terminationChannel chan bool
}

func CreateSFTPClient(ftptype FTPType, host string, port int, hostpath, filemask string, config *ftpConfiguration) (ConnectionInstance, error) {
	instance := &ftpServerInstance{
		ftpType:   ftptype,
		host:      host,
		port:      port,
		config:    config,
		hostpath:  hostpath,
		filemask:  filemask,
		isRunning: false,
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

func (conf *ftpConfiguration) LineBreakCR() *ftpConfiguration {
	conf.linebreak = LINEBREAK_CR
	return conf
}

func (conf *ftpConfiguration) LineBreakCRLF() *ftpConfiguration {
	conf.linebreak = LINEBREAK_CRLF
	return conf
}
func (conf *ftpConfiguration) LineBreakLF() *ftpConfiguration {
	conf.linebreak = LINEBREAK_LF
	return conf
}

func (conf *ftpConfiguration) DialTimeout(dialTimeout time.Duration) *ftpConfiguration {
	conf.dialTimeout = dialTimeout
	return conf
}

func (conf *ftpConfiguration) FilenameGeneratorPattern(pattern string) *ftpConfiguration {
	conf.autoGeneratedFilenames.generatorpattern = pattern
	return conf
}

func (conf *ftpConfiguration) FilenameGeneratorPrefix(prefix string) *ftpConfiguration {
	conf.autoGeneratedFilenames.prefix = prefix
	return conf
}

func (conf *ftpConfiguration) NoFilenamePrefix() *ftpConfiguration {
	conf.autoGeneratedFilenames.prefix = ""
	return conf
}

func (conf *ftpConfiguration) FilenameGeneratorSuffix(suffix string) *ftpConfiguration {
	conf.autoGeneratedFilenames.suffix = suffix
	return conf
}

func (conf *ftpConfiguration) NoFilenameSuffix() *ftpConfiguration {
	conf.autoGeneratedFilenames.suffix = ""
	return conf
}

func DefaultFTPConfig() *ftpConfiguration {
	ftpc := &ftpConfiguration{
		authMethod:      PASSWORD,
		pollInterval:    60 * time.Second,
		deleteAfterRead: true,
		dialTimeout:     10 * time.Second,
		linebreak:       LINEBREAK_LF,
	}
	ftpc.autoGeneratedFilenames.generatorpattern = "yyyyMMddhhmmss-#.dat"
	ftpc.autoGeneratedFilenames.prefix = "AUTO-"

	return ftpc
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
		handler.Error(instance, ErrorConfiguration, nil)
		log.Panic().Msg("Invalid (s)FTP Authentication-Method provided")
	}

	if instance.config.hostkey != "" {
		base64Key := []byte(instance.config.hostkey)
		key := make([]byte, base64.StdEncoding.DecodedLen(len(base64Key)))
		n, err := base64.StdEncoding.Decode(key, base64Key)
		if err != nil {
			handler.Error(instance, ErrorConfiguration, err)
			return
		}
		key = key[:n]
		hostKey, err := ssh.ParsePublicKey(key)
		if err != nil {
			handler.Error(instance, ErrorConfiguration, err)
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
		log.Error().Err(err).Msg("Open - Dial")
		handler.Error(instance, ErrorConfiguration, err)
		return
	}

	sftpClient, err := sftp.NewClient(sshConnection)
	if err != nil {
		log.Error().Err(err).Msg("Open - NewClient")
		sshConnection.Close()
		handler.Error(instance, ErrorConnect, err)
		return
	}

	if handler.Connected(instance) != nil {
		handler.Error(instance, ErrorConnect, err)
		return
	}

	ftpPath := strings.TrimRight(instance.hostpath, "/") + "/"

	instance.isRunning = true

	for instance.isRunning {

		files, err := sftpClient.ReadDir(ftpPath)
		if err != nil {
			log.Error().Err(err).Msgf("ReadFiles - ReadDir %s", ftpPath)
			handler.Error(instance, ErrorReceive, err)
			continue
		}

		if len(files) == 0 {
			continue
		}

		for _, file := range files {

			matchFileFilter, err := sftp.Match(strings.ToUpper(instance.filemask), strings.ToUpper(file.Name()))
			if err != nil {
				log.Error().Err(err).Msgf("ReadFiles - Match %s on %s ", strings.ToUpper(instance.filemask), strings.ToUpper(file.Name()))
				handler.Error(instance, ErrorReceive, err)
				continue
			}

			if matchFileFilter {

				filename := fmt.Sprintf("%s%s", ftpPath, file.Name())

				fileread, err := sftpClient.Open(filename)
				if err != nil {
					log.Error().Err(err).Msgf("ReadFiles - Open file %s@%s:%s%s", instance.config.user, instance.host, ftpPath, file.Name())
					handler.Error(instance, ErrorReceive, err)
					continue
				}
				filebuffer, err := ioutil.ReadAll(fileread)
				if err != nil {
					log.Error().Err(err).Msg("ReadFiles - ReadAll")
					handler.Error(instance, ErrorReceive, err)
					continue
				}

				err = fileread.Close()
				if err != nil {
					log.Error().Err(err).Msgf("ReadFiles - Close %s%s", ftpPath, file.Name())
					handler.Error(instance, ErrorReceive, err)
					continue
				}

				// only when err is null the data was successfully processed by user
				err = handler.DataReceived(instance, filebuffer, file.ModTime())

				if err == nil && instance.config.deleteAfterRead { // if processing was successful
					err := sftpClient.Remove(filename)
					if err != nil {
						log.Error().Err(err).Msg(filename)
						handler.Error(instance, ErrorInternal, err)
					}
				}

			}
		}

		select {
		case <-time.After(instance.config.pollInterval):
		case <-instance.terminationChannel:
			instance.isRunning = false
		}
	}
}

func (instance *ftpServerInstance) runWithFTP(handler Handler) {

	var err error

	ftpClient, err := ftp.Dial(fmt.Sprintf("%s:%d", instance.host, instance.port), ftp.DialWithTimeout(instance.config.dialTimeout))
	if err != nil {
		log.Error().Err(err).Msgf("Open - Dial Server : %s:%d", instance.host, instance.port)
		handler.Error(instance, ErrorLogin, err)
		return
	}

	if instance.config.authMethod != PASSWORD {
		handler.Error(instance, ErrorConfiguration, errors.New("Invalid Authentication Method"))
		log.Fatal().Msg("invalid authentication Method")
		return
	}

	err = ftpClient.Login(instance.config.user, instance.config.key)
	if err != nil {
		log.Error().Err(err).Msgf("Open - Login to %s as '%s'", instance.host, instance.config.user)
		handler.Error(instance, ErrorLogin, err)
		return
	}

	err = handler.Connected(instance)
	if err != nil {
		handler.Error(instance, ErrorLogin, err)
		return
	}

	ftpPath := strings.TrimRight(instance.hostpath, "/") + "/"

	instance.isRunning = true

	for instance.isRunning {

		files, err := ftpClient.List(ftpPath)
		if err != nil {
			log.Error().Err(err).Msgf("ReadFiles - List %s@%s:%s", instance.config.user, instance.host, ftpPath)
			handler.Error(instance, ErrorReceive, err)
			continue
		}

		for _, file := range files {

			match, err := sftp.Match(strings.ToUpper(instance.filemask), strings.ToUpper(file.Name))
			if err != nil {
				log.Error().Err(err).Msgf("ReadFiles - %s@%s Match '%s' '%s'", instance.config.user, instance.host, instance.filemask, file.Name)
				handler.Error(instance, ErrorInternal, err)
				continue
			}

			if match {
				log.Info().Msgf("readsFtpOrders %s@%s:%s", instance.config.user, instance.host, file.Name)

				// Rename to .processing
				err := ftpClient.Rename(ftpPath+file.Name, ftpPath+file.Name+".processing")
				if err != nil {
					log.Error().Err(err).Msgf("RenameFile %s@%s:%s", instance.config.user, instance.host, file.Name)
					handler.Error(instance, ErrorReceive, err)
					continue
				}

				// Read the content
				fileread, err := ftpClient.Retr(ftpPath + file.Name + ".processing")
				if err != nil {
					log.Error().Err(err).Msgf("ReadFiles - Retrieve %s@%s:%s", instance.config.user, instance.host, file.Name)
					handler.Error(instance, ErrorReceive, err)
					continue
				}
				filebuffer, err := ioutil.ReadAll(fileread)
				if err != nil {
					log.Error().Err(err).Msgf("ReadFiles - ReadAll %s@%s:%s", instance.config.user, instance.host, file.Name)
					handler.Error(instance, ErrorReceive, err)
					continue
				}

				err = fileread.Close()
				if err != nil {
					log.Error().Err(err).Msgf("ReadFiles - Close %s@%s:%s", instance.config.user, instance.host, file.Name)
					handler.Error(instance, ErrorReceive, err)
					continue
				}

				err = handler.DataReceived(instance, filebuffer, file.Time)

				if err == nil && instance.config.deleteAfterRead { // if processing was successful
					err := ftpClient.Delete(ftpPath + file.Name + ".processing")
					if err != nil {
						log.Error().Err(err).Msg(ftpPath + file.Name + ".processing")
						handler.Error(instance, ErrorReceive, err)
					}
				}
			}
		}

		select {
		case <-time.After(instance.config.pollInterval):
		case <-instance.terminationChannel:
			instance.isRunning = false
		}
	}
}

func (instance *ftpServerInstance) IsAlive() bool {
	return instance.isRunning
}

func (instance *ftpServerInstance) Send(msg [][]byte) (int, error) {
	var err error
	var bytesWritten int
	switch instance.ftpType {
	case FTP:
		bytesWritten, err = instance.ftpSend(msg)
	case SFTP:
		bytesWritten, err = instance.sftpSend(msg)
	default:
		// This should never execute
	}
	return bytesWritten, err
}

func (instance *ftpServerInstance) ftpSend(msg [][]byte) (int, error) {

	var err error

	ftpClient, err := ftp.Dial(fmt.Sprintf("%s:%d", instance.host, instance.port), ftp.DialWithTimeout(instance.config.dialTimeout))
	if err != nil {
		log.Error().Err(err).Msg("Open - Dial")
		return -1, err
	}

	if instance.config.authMethod != PASSWORD {
		log.Fatal().Msg("invalid authentication Method")
		return -1, err
	}

	err = ftpClient.Login(instance.config.user, instance.config.key)
	if err != nil {
		log.Error().Err(err).Msg("Open - Login")
		return -1, err
	}

	ftpPath := strings.TrimRight(instance.hostpath, "/") + "/"

	instance.config.autoGeneratedFilenames.counter += 1
	generatedFilename := instance.generateFileName(instance.config.autoGeneratedFilenames.generatorpattern,
		instance.config.autoGeneratedFilenames.prefix,
		instance.config.autoGeneratedFilenames.suffix,
		instance.config.autoGeneratedFilenames.counter)

	databytes := instance.addLineEndings(msg, instance.config.linebreak)

	err = ftpClient.Stor(ftpPath+generatedFilename, bytes.NewReader(databytes))
	if err != nil {
		log.Error().Err(err).Msg("WriteFtpResults - Stor")
		return -1, err
	}

	return -1, nil
}

func (instance *ftpServerInstance) sftpSend(msg [][]byte) (int, error) {

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
		return -1, errors.New("invalid (s)FTP Authentication-Method provided")
	}

	if instance.config.hostkey != "" {
		base64Key := []byte(instance.config.hostkey)
		key := make([]byte, base64.StdEncoding.DecodedLen(len(base64Key)))
		n, err := base64.StdEncoding.Decode(key, base64Key)
		if err != nil {
			return -1, err
		}
		key = key[:n]
		hostKey, err := ssh.ParsePublicKey(key)
		if err != nil {
			return -1, err
		}
		sshConfig.HostKeyCallback = ssh.FixedHostKey(hostKey)
	} else {
		sshConfig.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	}

	sshConfig.Timeout = instance.config.dialTimeout

	ftpserver := fmt.Sprintf("%s:%d", instance.host, instance.port)

	sshConnection, err := ssh.Dial("tcp", ftpserver, sshConfig)
	if err != nil {
		log.Error().Err(err).Msg("Open - Dial")
		return -1, err
	}

	sftpClient, err := sftp.NewClient(sshConnection)
	if err != nil {
		log.Error().Err(err).Msg("Open - NewClient")
		sshConnection.Close()
		return -1, err
	}

	ftpPath := strings.TrimRight(instance.hostpath, "/") + "/"

	databytes := instance.addLineEndings(msg, instance.config.linebreak)

	instance.config.autoGeneratedFilenames.counter += 1
	generatedFilename := instance.generateFileName(instance.config.autoGeneratedFilenames.generatorpattern,
		instance.config.autoGeneratedFilenames.prefix,
		instance.config.autoGeneratedFilenames.suffix,
		instance.config.autoGeneratedFilenames.counter)

	dstFile, err := sftpClient.Create(ftpPath + generatedFilename)
	if err != nil {
		return -1, err
	}
	bytesWritten, err := dstFile.Write(databytes)
	if err != nil {
		return -1, err
	}
	dstFile.Close()

	return bytesWritten, nil
}

func (instance *ftpServerInstance) addLineEndings(msg [][]byte, linebreak LINEBREAK) []byte {

	var lbreak []byte

	switch instance.config.linebreak {
	case LINEBREAK_CR:
		lbreak = []byte{0x0d}
	case LINEBREAK_CRLF:
		lbreak = []byte{0x0d, 0x0a}
	case LINEBREAK_LF:
		lbreak = []byte{0x0a}
	default:
		// this should never happen
		log.Error().Msg("Invalid linebreak configuration")
		lbreak = []byte{0x0a}
	}

	databytes := make([]byte, 0)
	for i := 0; i < len(msg); i++ {
		databytes = append(databytes, msg[i]...)
		databytes = append(databytes, lbreak...)
	}

	return databytes
}

func (instance *ftpServerInstance) generateFileName(filename, prefix, suffix string, cnt int) string {

	now := time.Now()

	filename = strings.Replace(filename, "yyyy", strconv.Itoa(now.Year()), -1)
	filename = strings.Replace(filename, "yy", strconv.Itoa(now.Year())[2:], -1)
	filename = strings.Replace(filename, "MM", fmt.Sprintf("%02d", int(now.Month())), -1)
	filename = strings.Replace(filename, "dd", fmt.Sprintf("%02d", now.Day()), -1)
	filename = strings.Replace(filename, "hh", fmt.Sprintf("%02d", now.Hour()), -1)
	filename = strings.Replace(filename, "mm", fmt.Sprintf("%02d", now.Minute()), -1)
	filename = strings.Replace(filename, "ss", fmt.Sprintf("%02d", now.Second()), -1)
	filename = strings.Replace(filename, "cc", fmt.Sprintf("%03d", now.Nanosecond()/1000), -1)
	filename = strings.Replace(filename, "nn", fmt.Sprintf("%06d", now.Nanosecond()), -1)

	filename = strings.Replace(filename, "#", fmt.Sprintf("%d", cnt), -1)

	if suffix != "" {
		filename = "." + strings.TrimLeft(".", suffix)
	}

	return prefix + filename
}

func (instance *ftpServerInstance) Receive() ([]byte, error) {
	return []byte{}, errors.New("Not implemented, use Run(handler) method and asyncronous method from handler")
}

// should call isAlive() for confirmation of a successfull termination
func (instance *ftpServerInstance) Close() error {
	if instance.IsAlive() {
		instance.terminationChannel <- true
	}
	return nil // anything else freezes
}

func (instance *ftpServerInstance) WaitTermination() error {
	return errors.New("Not implemented")
}

func (instance *ftpServerInstance) RemoteAddress() (string, error) {
	return instance.host, nil
}
