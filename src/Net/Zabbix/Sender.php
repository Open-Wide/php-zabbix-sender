<?php

namespace Net\Zabbix;

use Net\Zabbix\Exception\SenderNetworkException;
use Net\Zabbix\Exception\SenderProtocolException;

class Sender {

    private $_servername;
    private $_serverport;

    private $_timeout = 30;

    private $_protocolHeaderString = 'ZBXD';
    private $_protocolVersion = 1;

    private $_lastResponseInfo = null;
    private $_lastResponseArray = null;
    private $_lastProcessed = null;
    private $_lastFailed = null;
    private $_lastSpent = null;
    private $_lastTotal = null;

    private $_socket;
    private $_data;

    /**
     * __construct
     *
     * @param  string  $servername
     * @param  integer $serverport
     * @return void
     */
    function __construct( $servername = 'localhost', $serverport = 10051 ) {
        $this->setServerName( $servername );
        $this->setServerPort( $serverport );
        $this->initData( );
    }

    function initData( ) {
        $this->_data = array( );
    }

    function importAgentConfig( Agent\Config $agentConfig ) {
        $this->setServerName( $agentConfig->getServer( ) );
        $this->setServerPort( $agentConfig->getServerPort( ) );
        return $this;
    }

    function setServerName( $servername ) {
        $this->_servername = $servername;
        return $this;
    }

    function setServerPort( $serverport ) {
        if( is_int( $serverport ) ) {
            $this->_serverport = $serverport;
        }
        return $this;
    }

    function setTimeout( $timeout = 0 ) {
        if( (is_int( $timeout ) or is_numeric( $timeout )) and intval( $timeout ) > 0 ) {
            $this->_timeout = $timeout;
        }
        return $this;
    }

    function getTimeout( ) {
        return $this->_timeout;
    }

    function setProtocolHeaderString( $headerString ) {
        $this->_protocolHeaderString = $headerString;
        return $this;
    }

    function setProtocolVersion( $version ) {
        if( is_int( $version ) and $version > 0 ) {
            $this->_protocolVersion = $version;
        }
        return $this;
    }

    function addData( $hostname = null, $key = null, $value = null, $clock = null ) {
        $input = array(
            "host" => $hostname,
            "value" => $value,
            "key" => $key
        );
        if( isset( $clock ) ) {
            $input{"clock"} = $clock;
        }
        array_push( $this->_data, $input );
        return $this;
    }

    function getDataArray( ) {
        return $this->_data;
    }

    private function _buildSendData( ) {
        $request_array = array( );
        $data = $this->_data;
        $data = array(
            "request" => "sender data",
            "data" => $this->_data
        );
        $json_data = json_encode( array_map( function( $t ) {
            return is_string( $t ) ? utf8_encode( $t ) : $t;
        }, $data ) );
        $json_length = strlen( $json_data );
        $json_length_header = sprintf( "%04x", $json_length );

        $request = "ZBXD" . pack( "C", 1 );
        foreach( self::u64le( $json_length ) as $i ) {
            $request .= pack( "C", $i );
        }
        $request .= $json_data;
        return $request;
    }

    protected function _parseResponseInfo( $info = null ) {
        # info: "Processed 1 Failed 1 Total 2 Seconds spent 0.000035"
        $parsedInfo = null;
        if( isset( $info ) ) {
            list( , $processed, , $failed, , $total, , , $spent ) = explode( " ", $info );
            $parsedInfo = array(
                "processed" => intval( $processed ),
                "failed" => intval( $failed ),
                "total" => intval( $total ),
                "spent" => $spent,
            );
        }
        return $parsedInfo;
    }

    function getLastResponseInfo( ) {
        return $this->_lastResponseInfo;
    }

    function getLastResponseArray( ) {
        return $this->_lastResponseArray;
    }

    function getLastProcessed( ) {
        return $this->_lastProcessed;
    }

    function getLastFailed( ) {
        return $this->_lastFailed;
    }

    function getLastSpent( ) {
        return $this->_lastSpent;
    }

    function getLastTotal( ) {
        return $this->_lastTotal;
    }

    private function _clearLastResponseData( ) {
        $this->_lastResponseInfo = null;
        $this->_lastResponseArray = null;
        $this->_lastProcessed = null;
        $this->_lastFailed = null;
        $this->_lastSpent = null;
        $this->_lastTotal = null;
    }

    private function _close( ) {
        if( $this->_socket ) {
            fclose( $this->_socket );
        }
    }

    /**
     * connect to Zabbix Server
     * @throws Net\Zabbix\Exception\SenderNetworkException
     *
     */
    private function _connect( ) {
        $this->_socket = fsockopen( $this->_servername, intval( $this->_serverport ), $errno, $errmsg, $this->_timeout );
        //$serverURL = sprintf( "tcp://%s:%d", $this->_servername, $this->_serverport );
        //$this->_socket = stream_socket_client( $serverURL, $errno, $errstr, $this->_timeout, STREAM_CLIENT_CONNECT );
        if( !is_resource( $this->_socket ) || $errno != 0 ) {
            throw new SenderNetworkException( sprintf( '%s,%s', $errno, $errmsg ) );
        }
    }

    /**
     * write data to socket
     * @throws Net\Zabbix\Exception\SenderNetworkException
     *
     */
    private function _write( $data ) {
        if( !is_resource( $this->_socket ) ) {
            throw new SenderNetworkException( 'socket was not writable,connect failed.' );
        }
        $totalWritten = 0;
        $length = strlen( $data );
        while( $totalWritten < $length ) {
            $writeSize = fwrite( $this->_socket, $data );
            $errorcode = socket_last_error( );
            $errormsg = socket_strerror( $errorcode );
            if( $writeSize === false ) {
                return false;
            } else {
                $totalWritten += $writeSize;
                $data = substr( $data, $writeSize );
            }
        }
        return $totalWritten;
    }

    /**
     * read data from socket
     * @throws Net\Zabbix\Exception\SenderNetworkException
     *
     */
    private function _read( ) {
        if( !is_resource( $this->_socket ) ) {
            throw new SenderNetworkException( 'socket was not readable,connect failed.' );
        }
        $recvData = "";
        while( !feof( $this->_socket ) ) {
            $buffer = fread( $this->_socket, 8192 );
            if( $buffer === false ) {
                return false;
            }
            $recvData .= $buffer;
        }
        return $recvData;
    }

    /**
     * main
     * @throws Net\Zabbix\Exception\SenderNetworkException
     * @throws Net\Zabbix\Exception\SenderProtocolException
     *
     */
    function send( ) {
        $this->_clearLastResponseData( );
        $sendData = $this->_buildSendData( );
        $sendSucceed = TRUE;
        
        $datasize = strlen( $sendData );

        $this->_connect( );

        /* send data to zabbix server */
        $sentsize = $this->_write( $sendData );
        if( $sentsize === false or $sentsize != $datasize ) {
            throw new SenderNetworkException( 'cannot receive response' );
        }

        /* receive data from zabbix server */
        $recvData = $this->_read( );
        if( $recvData === false ) {
            throw new SenderNetworkException( 'cannot receive response' );
        }

        $this->_close( );

        $recvProtocolHeader = substr( $recvData, 0, 4 );
        if( $recvProtocolHeader == "ZBXD" ) {
            $responseData = substr( $recvData, 13 );
            $responseArray = json_decode( $responseData, true );
            if( is_null( $responseArray ) ) {
                throw new SenderProtocolException( 'invalid json data in receive data' );
            }
            $this->_lastResponseArray = $responseArray;
            $this->_lastResponseInfo = $responseArray{'info'};
            $parsedInfo = $this->_parseResponseInfo( $this->_lastResponseInfo );
            $this->_lastProcessed = $parsedInfo{'processed'};
            $this->_lastFailed = $parsedInfo{'failed'};
            $this->_lastSpent = $parsedInfo{'spent'};
            $this->_lastTotal = $parsedInfo{'total'};
            if( $responseArray{'response'} != "success" ) {
                $sendSucceed = FALSE;
            }
        } elseif( $recvProtocolHeader == "OK" ) {
            throw new SenderNetworkException( 'Request is too long. Request size : ' . $sentsize );
        } else {
            throw new SenderNetworkException( 'Invalid response : ' . $recvProtocolHeader );
        }
        if( $sendSucceed ) {
            $this->initData( );
            return true;
        } else {
            $this->_clearLastResponseData( );
            return false;
        }
    }

    protected function u64le( $integer ) {
        $ary = array( );
        for( $i = 0; $i < 4; $i++ ) {
            $ary[] = (($integer>>($i * 8)) & 0xFF);
        }
        for( $i = 0; $i < 4; $i++ ) {
            $ary[] = 0;
        }
        return $ary;
    }

}
