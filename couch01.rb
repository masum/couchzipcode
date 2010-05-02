# -*- coding: utf-8 -*-
require 'couchlib'
require 'csv'
require 'nkf'
require 'uri'
require 'kconv'
require 'jcode'
$KCODE = 'UTF8'

$dbname = "suggest01"
$csvname = "KEN_ALL.CSV"
#$csvname = "22SHIZUO.CSV"
$couch = CouchDB::Client.new("http://127.0.0.1/couchdb/")
$count = 0

#---------------------------------------------------
# ＤＢ作成
#---------------------------------------------------
p res = $couch.get($dbname)
unless (res['error'] == nil) then
  p $couch.put($dbname,'')
end

#---------------------------------------------------
# ＭＡＰ関数登録
#---------------------------------------------------
design = Hash::new
design = { 
 "_id"=>"_design/suggest",
 "language"=>"javascript",
 "views"=> { 
    "suggest"=>{ 
      "map"=>"function(doc) {
         emit(doc.keyword,{data:doc.data,hanzi:doc.hanzi});
      }",
    }
  }
}
db = $dbname+URI.escape("/_design/suggest")
p $couch.put(db,design)


#---------------------------------------------------
# データ登録
#---------------------------------------------------
$bulk = Hash::new
$bulk["docs"] = []


def flashData()
  if ($bulk['docs'].length > 0) then
    $bulk['docs'].uniq!
    p $bulk['docs'].length
    db = $dbname+"/_bulk_docs"
    db = URI.escape(db)
    $couch.post(db,$bulk)
    $bulk['docs'] = []
  end
end

def appendData(key,value,hanzi)
  $count += 1
  p $count if (($count % 100000)==0)
  #p key + "," + value
  data = {}
  data['keyword'] = key
  data['data'] = value
  data['hanzi'] = hanzi
  $bulk['docs'].push(data)
  if ($bulk['docs'].length == 100000) then
    flashData()
  end
end

#
# [しずおかけん], 静岡県
#
def makeKV1(param)
  ar = param['key1'].split(//)
  cnt = 0
  while cnt<ar.length
    key = ar[0..cnt].join
    appendData(key, param['value1'],"")
    cnt += 1
  end
end

#
# [しずおかけんしずおかし], 静岡県静岡市
#
def makeKV2(param)
  ar = param['key2'].split(//)
  cnt = 0
  while cnt<ar.length
    key = param['key1']+ar[0..cnt].join
    appendData(key, param['value1']+param['value2'],"")
    cnt += 1
  end
end

#
# [しずおかけんしずおかしみなみちょう], 静岡県静岡市南町
#
def makeKV3(param)
  ar = param['key3'].split(//)
  cnt = 0
  while cnt<ar.length
    key = param['key1']+param['key2']+ar[0..cnt].join
    appendData(key, param['value1']+param['value2']+param['value3'],"")
    cnt += 1
  end
end

#
# 静岡県[しずおかし], 静岡県静岡市
#
def makeKV4(param)
  ar = param['key2'].split(//)
  cnt = 0
  while cnt<ar.length
    key = param['value1']+ar[0..cnt].join
    appendData(key, param['value1']+param['value2'],"")
    cnt += 1
  end
end

#
# 静岡県静岡市[かまた], 静岡県静岡市鎌田
#
def makeKV5(param)
  ar = param['key3'].split(//)
  cnt = 0
  while cnt<ar.length
    key = param['value1']+param['value2']+ar[0..cnt].join
    appendData(key, param['value1']+param['value2']+param['value3'],"")
    cnt += 1
  end
end

#
# [静岡県], 静岡県
#
def makeKV6(param)
  ar = param['value1'].split(//)
  cnt = 0
  while cnt<ar.length
    key = ar[0..cnt].join
    appendData(key, param['value1'],"")
    cnt += 1
  end
end

#
# 静岡県[静岡市], 静岡市
#
def makeKV7(param)
  ar = param['value2'].split(//)
  cnt = 0
  while cnt<ar.length
    key = param['value1']+ar[0..cnt].join
    appendData(key, param['value2'],param['value1']+param['value2'])
    cnt += 1
  end
end

#
# 静岡県静岡市[鎌田], 鎌田
#
def makeKV8(param)
  ar = param['value3'].split(//)
  cnt = 0
  while cnt<ar.length
    key = param['value1']+param['value2']+ar[0..cnt].join
    appendData(key, param['value3'],param['value1']+param['value2']+param['value3'])
    cnt += 1
  end
end

CSV.open($csvname, "r") do |row|
  adr = {}
  adr['key1'] = NKF.nkf('-Sw -Lu -h', row[3])
  adr['key2'] = NKF.nkf('-Sw -Lu -h', row[4])
  adr['key3'] = NKF.nkf('-Sw -Lu -h', row[5])
  #adr['key3'].scan(/^([^0-9]*)[0-9]*/)
  #adr['key3'] = $1
  adr['key3'] = adr['key3'].split('(')[0]
  
  adr['value1'] = row[6].toutf8
  adr['value2'] = row[7].toutf8
  adr['value3'] = row[8].toutf8
  unless adr['value3'].scan(/以下に/).length == 0 
    next
  end
  makeKV1(adr)
  makeKV2(adr)
  makeKV3(adr)
  makeKV4(adr)
  makeKV5(adr)
  makeKV6(adr)
  makeKV7(adr)
  makeKV8(adr)
end
flashData()

