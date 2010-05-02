# -*- coding: utf-8 -*-
require 'rubygems'
require 'httpclient'
require 'json/lexer'
require 'csv'
require 'nkf'
require 'uri'
# CouchDBクライアント
module CouchDB
  class Client
    def initialize(host, proxy=ENV["http_proxy"])
      @host = host
      @client = HTTPClient.new( proxy, "http client." )
    end
    def delete(uri, value=nil)
      request( :delete, uri, value.to_json)
    end
    def get(uri)
      request( :get, uri)
    end
    def head(uri)
      begin
        request( :head, uri)
        return true
      rescue
        return false if $!.to_s =~/^404/
        raise $!
      end
    end
    def put(uri, value)
      request( :put, uri, value.to_json)
    end
    def post(uri, value)
      request( :post, uri, value.to_json)
    end
    def request(method,uri,body=nil)
      p url = "#{@host}/#{uri}"
      res = @client.request( method, url,
        body ? {"content-type"=>"application/json"} : nil, body)
      error( res, method, uri, body ) if res.status >= 400
      JSON::Lexer.new(res.content).nextvalue if res.content rescue nil
    end
  private
    def error( res, method, uri, body )
      message = ""
      if res.content
        parsed = JSON::Lexer.new(res.content).nextvalue
        message = "#{parsed["error"]} : #{parsed["reason"]}" if parsed
      end
      # raise "#{res.status} : #{method} #{uri} #{message} \n#{body}"
    end
  end
end
