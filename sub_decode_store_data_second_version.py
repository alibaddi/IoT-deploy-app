import gzip
import ssl
import paho.mqtt.client as paho
import paho.mqtt.subscribe as subscribe
import psycopg2
from paho import mqtt
import struct

empty_table_LXeq = False
empty_table_LXY = False
empty_table_LXeqT = False
empty_table_LN = False
empty_table_SPECTRUMT = False
empty_table_LXET = False
empty_table_TEMPORAL = False

# Set up broker parameters :
hostname = "ca50520c22274ff88b92c910a1e2af5e.s2.eu.hivemq.cloud"
port = 8883
user = "alidelhom"
pwd = "ZyG@9A#zZdwkaG5"
client_name = 'sub_python'
topic = "bot_acoustic/SN1"

c_input_msg = 0
c_output_msg = 0


def bytes_to_int(x):
    try:
        return int.from_bytes(x, 'big')
    except:
        print(x)


def get_trameID_data(trameID, msg):
    if trameID == 8:
        LAeq = bytes_to_int(msg[1:3]) / 10
        LCeq = bytes_to_int(msg[3:5]) / 10
        LZeq = bytes_to_int(msg[5:7]) / 10
        return msg[7:], {"LXeq": [LAeq, LCeq, LZeq]}
    elif trameID == 2:
        LAS = bytes_to_int(msg[1:3]) / 10
        LCS = bytes_to_int(msg[3:5]) / 10
        LZS = bytes_to_int(msg[5:7]) / 10
        Lpeak = bytes_to_int(msg[7:9]) / 10
        return msg[9:], {"LXY": [LAS, LCS, LZS, Lpeak]}
    elif trameID == 4:
        LAeqT = bytes_to_int(msg[1:3]) / 10
        LCeqT = bytes_to_int(msg[3:5]) / 10
        LZeqT = bytes_to_int(msg[5:7]) / 10
        LAeqTmax = bytes_to_int(msg[7:9]) / 10
        LCeqTmax = bytes_to_int(msg[9:11]) / 10
        LZeqTmax = bytes_to_int(msg[11:13]) / 10
        LAeqTmin = bytes_to_int(msg[13:15]) / 10
        LCeqTmin = bytes_to_int(msg[15:17]) / 10
        LZeqTmin = bytes_to_int(msg[17:19]) / 10
        return msg[19:], {"LXeqT": [LAeqT, LCeqT, LZeqT, LAeqTmax, LCeqTmax, LZeqTmax, LAeqTmin, LCeqTmin, LZeqTmin]}
    elif trameID == 5:
        Ln1 = bytes_to_int(msg[1:3]) / 10
        Ln2 = bytes_to_int(msg[3:5]) / 10
        Ln3 = bytes_to_int(msg[5:7]) / 10
        Ln4 = bytes_to_int(msg[7:9]) / 10
        Ln5 = bytes_to_int(msg[9:11]) / 10
        Ln6 = bytes_to_int(msg[11:13]) / 10
        Ln7 = bytes_to_int(msg[13:15]) / 10
        return msg[15:], {"LN": [Ln1, Ln2, Ln3, Ln4, Ln5, Ln6, Ln7]}
    elif trameID == 9:
        octave_16 = bytes_to_int(msg[1:3]) / 10
        octave_31 = bytes_to_int(msg[3:5]) / 10
        octave_63 = bytes_to_int(msg[5:7]) / 10
        octave_125 = bytes_to_int(msg[7:9]) / 10
        octave_250 = bytes_to_int(msg[9:11]) / 10
        octave_500 = bytes_to_int(msg[11:13]) / 10
        octave_1000 = bytes_to_int(msg[13:15]) / 10
        octave_2000 = bytes_to_int(msg[15:17]) / 10
        octave_4000 = bytes_to_int(msg[17:19]) / 10
        octave_8000 = bytes_to_int(msg[19:21]) / 10
        octave_16000 = bytes_to_int(msg[21:23]) / 10
        third_octave_10 = bytes_to_int(msg[23:25]) / 10
        third_octave_12 = bytes_to_int(msg[25:27]) / 10
        third_octave_16 = bytes_to_int(msg[27:29]) / 10
        third_octave_20 = bytes_to_int(msg[29:31]) / 10
        third_octave_25 = bytes_to_int(msg[31:33]) / 10
        third_octave_31 = bytes_to_int(msg[33:35]) / 10
        third_octave_40 = bytes_to_int(msg[35:37]) / 10
        third_octave_50 = bytes_to_int(msg[37:39]) / 10
        third_octave_63 = bytes_to_int(msg[39:41]) / 10
        third_octave_80 = bytes_to_int(msg[41:43]) / 10
        third_octave_100 = bytes_to_int(msg[43:45]) / 10
        third_octave_125 = bytes_to_int(msg[45:47]) / 10
        third_octave_160 = bytes_to_int(msg[47:49]) / 10
        third_octave_200 = bytes_to_int(msg[49:51]) / 10
        third_octave_250 = bytes_to_int(msg[51:53]) / 10
        third_octave_315 = bytes_to_int(msg[53:55]) / 10
        third_octave_400 = bytes_to_int(msg[55:57]) / 10
        third_octave_500 = bytes_to_int(msg[57:59]) / 10
        third_octave_630 = bytes_to_int(msg[59:61]) / 10
        third_octave_800 = bytes_to_int(msg[61:63]) / 10
        third_octave_1000 = bytes_to_int(msg[63:65]) / 10
        third_octave_1250 = bytes_to_int(msg[65:67]) / 10
        third_octave_1600 = bytes_to_int(msg[67:69]) / 10
        third_octave_2000 = bytes_to_int(msg[69:71]) / 10
        third_octave_2500 = bytes_to_int(msg[71:73]) / 10
        third_octave_3150 = bytes_to_int(msg[73:75]) / 10
        third_octave_4000 = bytes_to_int(msg[75:77]) / 10
        third_octave_5000 = bytes_to_int(msg[77:79]) / 10
        third_octave_6300 = bytes_to_int(msg[79:81]) / 10
        third_octave_8000 = bytes_to_int(msg[81:83]) / 10
        third_octave_10000 = bytes_to_int(msg[83:85]) / 10
        third_octave_12500 = bytes_to_int(msg[85:87]) / 10
        third_octave_16000 = bytes_to_int(msg[87:89]) / 10
        third_octave_20000 = bytes_to_int(msg[89:91]) / 10
        return msg[91:], {"SPECTRUMT": [octave_16, octave_31, octave_63, octave_125, octave_250, octave_500,
                                        octave_1000, octave_2000, octave_4000, octave_8000, octave_16000,
                                        third_octave_10, third_octave_12, third_octave_16, third_octave_20,
                                        third_octave_25, third_octave_31, third_octave_40, third_octave_50,
                                        third_octave_63, third_octave_80, third_octave_100, third_octave_125,
                                        third_octave_160, third_octave_200, third_octave_250, third_octave_315,
                                        third_octave_400, third_octave_500, third_octave_630, third_octave_800,
                                        third_octave_1000, third_octave_1250, third_octave_1600, third_octave_2000,
                                        third_octave_2500, third_octave_3150, third_octave_4000, third_octave_5000,
                                        third_octave_6300, third_octave_8000, third_octave_10000, third_octave_12500,
                                        third_octave_16000, third_octave_20000]}
    elif trameID == 6:
        LAET = bytes_to_int(msg[1:3]) / 10
        LCET = bytes_to_int(msg[3:5]) / 10
        LZET = bytes_to_int(msg[5:7]) / 10
        return msg[7:], {"LXET": [LAET, LCET, LZET]}
    elif trameID == 7:
        LXeqTs = bytes_to_int(msg[1:3]) / 10
        LXeqhh1 = bytes_to_int(msg[3:5]) / 10
        LXeqhh2 = bytes_to_int(msg[5:7]) / 10
        LXeqhh3 = bytes_to_int(msg[7:9]) / 10
        return msg[9:], {"TEMPORAL": [LXeqTs, LXeqhh1, LXeqhh2, LXeqhh3]}


# def decode_battery_percentage(byte_representation):
#     # Unpack the byte representation back into a float
#     float_value = struct.unpack('!f', byte_representation)[0]
#     # Convert the float value back to the battery percentage
#     battery_percentage = float_value
#     return battery_percentage

def decode_battery_percentage(byte_representation):
    # Convert the byte representation back to the battery percentage
    battery_percentage = int(int.from_bytes(byte_representation, byteorder='big') / 2.55)
    return battery_percentage
   

def decode_network_status(byte_representation):
    return int.from_bytes(byte_representation, byteorder='big')


def decode_msg(msg, msg_topic):
    hostname = 'localhost'
    database = 'IoT_Messages'
    username = 'postgres'
    pwd = 'delhom!12'
    port_id = 5432
    conn = None
    cur = None
    global empty_table_LXeq
    global empty_table_LXY
    global empty_table_LXeqT
    global empty_table_LN
    global empty_table_SPECTRUMT
    global empty_table_LXET
    global empty_table_TEMPORAL
    try:
        conn = psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id)
        cur = conn.cursor()

        create_script = ''' CREATE TABLE IF NOT EXISTS {}(
                                        id SERIAL PRIMARY KEY,
                                        TimeStamp    int,
                                        Topic   varchar(65535),
                                        LAeq    varchar(65535),
                                        LCeq    varchar(65535),
                                        LZeq    varchar(65535),
                                        LAS    varchar(65535),
                                        LCS    varchar(65535),
                                        LZS    varchar(65535),
                                        Lpeak    varchar(65535),
                                        LAeqT    varchar(65535),
                                        LCeqT    varchar(65535),
                                        LZeqT    varchar(65535),
                                        LAeqTmax    varchar(65535),
                                        LCeqTmax    varchar(65535),
                                        LZeqTmax    varchar(65535),
                                        LAeqTmin    varchar(65535),
                                        LCeqTmin    varchar(65535),
                                        LZeqTmin    varchar(65535),
                                        Ln1    varchar(65535),
                                        Ln2    varchar(65535),
                                        Ln3    varchar(65535),
                                        Ln4    varchar(65535),
                                        Ln5    varchar(65535),
                                        Ln6    varchar(65535),
                                        Ln7    varchar(65535),
                                        octave_16    varchar(65535),
                                        octave_31    varchar(65535),
                                        octave_63    varchar(65535),
                                        octave_125    varchar(65535),
                                        octave_250    varchar(65535),
                                        octave_500    varchar(65535),
                                        octave_1000    varchar(65535),
                                        octave_2000    varchar(65535),
                                        octave_4000    varchar(65535),
                                        octave_8000    varchar(65535),
                                        octave_16000    varchar(65535),
                                        third_octave_10    varchar(65535),
                                        third_octave_12    varchar(65535),
                                        third_octave_16    varchar(65535),
                                        third_octave_20    varchar(65535),
                                        third_octave_25    varchar(65535),
                                        third_octave_31    varchar(65535),
                                        third_octave_40    varchar(65535),
                                        third_octave_50    varchar(65535),
                                        third_octave_63    varchar(65535),
                                        third_octave_80    varchar(65535),
                                        third_octave_100    varchar(65535),
                                        third_octave_125    varchar(65535),
                                        third_octave_160    varchar(65535),
                                        third_octave_200    varchar(65535),
                                        third_octave_250    varchar(65535),
                                        third_octave_315    varchar(65535),
                                        third_octave_400    varchar(65535),
                                        third_octave_500    varchar(65535),
                                        third_octave_630    varchar(65535),
                                        third_octave_800    varchar(65535),
                                        third_octave_1000    varchar(65535),
                                        third_octave_1250    varchar(65535),
                                        third_octave_1600    varchar(65535),
                                        third_octave_2000    varchar(65535),
                                        third_octave_2500    varchar(65535),
                                        third_octave_3150    varchar(65535),
                                        third_octave_4000    varchar(65535),
                                        third_octave_5000    varchar(65535),
                                        third_octave_6300    varchar(65535),
                                        third_octave_8000    varchar(65535),
                                        third_octave_10000    varchar(65535),
                                        third_octave_12500    varchar(65535),
                                        third_octave_16000    varchar(65535),
                                        third_octave_20000    varchar(65535),
                                        LAET    varchar(65535),
                                        LCET    varchar(65535),
                                        LZET    varchar(65535),
                                        LXeqTs    varchar(65535),
                                        LXeqhh1    varchar(65535),
                                        LXeqhh2    varchar(65535),
                                        LXeqhh3    varchar(65535))'''.format(((str(msg_topic))[13:]) + '_AC')
        cur.execute(create_script)

        create_script = ''' CREATE TABLE IF NOT EXISTS {}(
                                                id SERIAL PRIMARY KEY,
                                                TimeStamp    int,
                                                Topic   varchar(65535),
                                                Battery    varchar(65535))'''.format(((str(msg_topic))[13:]) +
                                                                                     '_Battery')
        cur.execute(create_script)

        create_script = ''' CREATE TABLE IF NOT EXISTS {}(
                                                id SERIAL PRIMARY KEY,
                                                TimeStamp    int,
                                                Topic   varchar(65535),
                                                Network    varchar(65535))'''.format(((str(msg_topic))[13:]) +
                                                                                     '_Network')
        cur.execute(create_script)

        slicer_byte = msg[0:1]
        unix_byte = msg[1:5]
        print('header : {}'.format(bytes_to_int(slicer_byte)))
        print('unix : {}'.format(bytes_to_int(unix_byte)))
        print('Topic : ' + str(msg_topic))
        msg = msg[5:]

        while msg != bytes():
            if bytes_to_int(msg[0:1]) != 255:
                trameID = bytes_to_int(msg[0:1])
                msg, value = get_trameID_data(trameID, msg)
                print(value)

                ############################     LXeq     ############################

                if "LXeq" in value:
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, LAeq, LCeq, LZeq) VALUES (%s,%s,%s,%s,%s)'.format(
                        ((str(msg_topic))[13:]) + '_AC')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, value['LXeq'][0], value['LXeq'][1], value['LXeq'][2])
                    cur.execute(insert_script, insert_value)

                    conn.commit()

                ############################     LXY     ############################
                cur.execute("SELECT COUNT(*) FROM {}".format(((str(msg_topic))[13:]) + '_AC'))
                table_count = cur.fetchone()[0]

                if table_count == 0:
                    empty_table_LXY = True

                if ("LXY" in value) and (empty_table_LXY == True):
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, LAS, LCS, LZS, Lpeak) VALUES (%s,%s,' \
                                    '%s,%s,%s,%s) '.format(((str(msg_topic))[13:]) + '_AC')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, value['LXY'][0], value['LXY'][1], value['LXY'][2],
                        value['LXY'][3])
                    cur.execute(insert_script, insert_value)
                    empty_table_LXY = True
                    conn.commit()


                elif ("LXY" in value) and (empty_table_LXY == False):
                    cur.execute("ALTER TABLE {} ADD COLUMN IF NOT EXISTS updated BOOLEAN DEFAULT FALSE".format(
                        ((str(msg_topic))[13:]) + '_AC'))
                    conn.commit()

                    cur.execute("UPDATE {} SET LAS=%s, LCS=%s, LZS=%s, Lpeak=%s, updated=true WHERE TimeStamp=%s and "
                                "updated=false ".format(((str(msg_topic))[13:]) + '_AC'), (value['LXY'][0],
                                                                                           value['LXY'][1],
                                                                                           value['LXY'][2],
                                                                                           value['LXY'][3],
                                                                                           bytes_to_int(unix_byte)))
                    conn.commit()

                ############################     LXeqT     ############################
                cur.execute("SELECT COUNT(*) FROM {}".format(((str(msg_topic))[13:]) + '_AC'))
                table_count = cur.fetchone()[0]

                if table_count == 0:
                    empty_table_LXeqT = True

                if ("LXeqT" in value) and (empty_table_LXeqT == True):
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, LAeqT, LCeqT, LZeqT, LAeqTmax, LCeqTmax, ' \
                                    'LZeqTmax,LAeqTmin, LCeqTmin, LZeqTmin) VALUES (%s,%s,' \
                                    '%s,%s,%s,%s,%s,%s,%s,%s,%s) '.format(((str(msg_topic))[13:]) + '_AC')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, value['LXeqT'][0], value['LXeqT'][1], value['LXeqT'][2],
                        value['LXeqT'][3], value['LXeqT'][4], value['LXeqT'][5], value['LXeqT'][6],
                        value['LXeqT'][7],
                        value['LXeqT'][8])
                    cur.execute(insert_script, insert_value)
                    empty_table_LXeqT = True
                    conn.commit()

                elif ("LXeqT" in value) and (empty_table_LXeqT == False):
                    cur.execute(
                        "UPDATE {} SET LAeqT=%s, LCeqT=%s, LZeqT=%s, LAeqTmax=%s, LCeqTmax=%s, LZeqTmax=%s, "
                        "LAeqTmin=%s, LCeqTmin=%s, LZeqTmin=%s  WHERE TimeStamp=%s ".format(
                            ((str(msg_topic))[13:]) + '_AC'),
                        (
                            value['LXeqT'][0], value['LXeqT'][1], value['LXeqT'][2], value['LXeqT'][3],
                            value['LXeqT'][4], value['LXeqT'][5], value['LXeqT'][6], value['LXeqT'][7],
                            value['LXeqT'][8], bytes_to_int(unix_byte)))
                    conn.commit()

                ############################     LN     ############################
                cur.execute("SELECT COUNT(*) FROM {}".format(((str(msg_topic))[13:]) + '_AC'))
                table_count = cur.fetchone()[0]

                if table_count == 0:
                    empty_table_LN = True

                if ("LN" in value) and (empty_table_LN == True):
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, Ln1, Ln2, Ln3, Ln4, Ln5, Ln6, Ln7) VALUES ' \
                                    '(%s,%s,%s,%s,%s,%s,%s,%s,%s) '.format(((str(msg_topic))[13:]) + '_AC')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, value['LN'][0], value['LN'][1], value['LN'][2],
                        value['LN'][3], value['LN'][4], value['LN'][5], value['LN'][6])
                    cur.execute(insert_script, insert_value)
                    empty_table_LN = True
                    conn.commit()

                elif ("LN" in value) and (empty_table_LN == False):
                    cur.execute(
                        "UPDATE {} SET Ln1=%s, Ln2=%s, Ln3=%s, Ln4=%s, Ln5=%s, Ln6=%s, "
                        "Ln7=%s WHERE TimeStamp=%s ".format(((str(msg_topic))[13:]) + '_AC'),
                        (
                            value['LN'][0], value['LN'][1], value['LN'][2], value['LN'][3],
                            value['LN'][4], value['LN'][5], value['LN'][6], bytes_to_int(unix_byte)))
                    conn.commit()

                ############################     SPECTRUMT     ############################
                cur.execute("SELECT COUNT(*) FROM {}".format(((str(msg_topic))[13:]) + '_AC'))
                table_count = cur.fetchone()[0]

                if table_count == 0:
                    empty_table_SPECTRUMT = True

                if ("SPECTRUMT" in value) and (empty_table_SPECTRUMT == True):
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, octave_16, octave_31, octave_63, ' \
                                    'octave_125, octave_250, octave_500, octave_1000, octave_2000, octave_4000, ' \
                                    'octave_8000, octave_16000, third_octave_10, third_octave_12, third_octave_16, ' \
                                    'third_octave_20, third_octave_25, third_octave_31, third_octave_40, ' \
                                    'third_octave_50, third_octave_63, third_octave_80, third_octave_100, ' \
                                    'third_octave_125, third_octave_160, third_octave_200, third_octave_250, ' \
                                    'third_octave_315, third_octave_400, third_octave_500, third_octave_630, ' \
                                    'third_octave_800, third_octave_1000, third_octave_1250, third_octave_1600, ' \
                                    'third_octave_2000, third_octave_2500, third_octave_3150, third_octave_4000, ' \
                                    'third_octave_5000, third_octave_6300, third_octave_8000, third_octave_10000, ' \
                                    'third_octave_12500, third_octave_16000, third_octave_20000) VALUES ' \
                                    '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,' \
                                    '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '.format(
                        ((str(msg_topic))[13:]) + '_AC')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, value['SPECTRUMT'][0], value['SPECTRUMT'][1],
                        value['SPECTRUMT'][2], value['SPECTRUMT'][3], value['SPECTRUMT'][4], value['SPECTRUMT'][5],
                        value['SPECTRUMT'][6], value['SPECTRUMT'][7],
                        value['SPECTRUMT'][8], value['SPECTRUMT'][9], value['SPECTRUMT'][10],
                        value['SPECTRUMT'][11],
                        value['SPECTRUMT'][12], value['SPECTRUMT'][13], value['SPECTRUMT'][14],
                        value['SPECTRUMT'][15],
                        value['SPECTRUMT'][16], value['SPECTRUMT'][17], value['SPECTRUMT'][18],
                        value['SPECTRUMT'][19],
                        value['SPECTRUMT'][20], value['SPECTRUMT'][21], value['SPECTRUMT'][22],
                        value['SPECTRUMT'][23],
                        value['SPECTRUMT'][24], value['SPECTRUMT'][25], value['SPECTRUMT'][26],
                        value['SPECTRUMT'][27],
                        value['SPECTRUMT'][28], value['SPECTRUMT'][29], value['SPECTRUMT'][30],
                        value['SPECTRUMT'][31],
                        value['SPECTRUMT'][32], value['SPECTRUMT'][33], value['SPECTRUMT'][34],
                        value['SPECTRUMT'][35],
                        value['SPECTRUMT'][36], value['SPECTRUMT'][37], value['SPECTRUMT'][38],
                        value['SPECTRUMT'][39],
                        value['SPECTRUMT'][40], value['SPECTRUMT'][41], value['SPECTRUMT'][42],
                        value['SPECTRUMT'][43],
                        value['SPECTRUMT'][44])
                    cur.execute(insert_script, insert_value)
                    empty_table_SPECTRUMT = True
                    conn.commit()

                elif ("SPECTRUMT" in value) and (empty_table_SPECTRUMT == False):
                    cur.execute(
                        "UPDATE {} SET octave_16=%s, octave_31=%s, octave_63=%s, octave_125=%s, octave_250=%s, "
                        "octave_500=%s, octave_1000=%s, octave_2000=%s, octave_4000=%s, octave_8000=%s, "
                        "octave_16000=%s, "
                        "third_octave_10=%s, third_octave_12=%s, third_octave_16=%s, third_octave_20=%s, "
                        "third_octave_25=%s, "
                        "third_octave_31=%s, "
                        "third_octave_40=%s, third_octave_50=%s, third_octave_63=%s, third_octave_80=%s, "
                        "third_octave_100=%s, third_octave_125=%s, "
                        "third_octave_160=%s, third_octave_200=%s, third_octave_250=%s, third_octave_315=%s, "
                        "third_octave_400=%s, third_octave_500=%s, "
                        "third_octave_630=%s, third_octave_800=%s, third_octave_1000=%s, third_octave_1250=%s, "
                        "third_octave_1600=%s, third_octave_2000=%s, "
                        "third_octave_2500=%s, third_octave_3150=%s, third_octave_4000=%s, third_octave_5000=%s, "
                        "third_octave_6300=%s, third_octave_8000=%s, "
                        "third_octave_10000=%s, third_octave_12500=%s, third_octave_16000=%s, third_octave_20000=%s "
                        "WHERE TimeStamp=%s ".format(((str(msg_topic))[13:]) + '_AC'),
                        (
                            value['SPECTRUMT'][0], value['SPECTRUMT'][1], value['SPECTRUMT'][2], value['SPECTRUMT'][3],
                            value['SPECTRUMT'][4], value['SPECTRUMT'][5], value['SPECTRUMT'][6], value['SPECTRUMT'][7],
                            value['SPECTRUMT'][8], value['SPECTRUMT'][9], value['SPECTRUMT'][10],
                            value['SPECTRUMT'][11],
                            value['SPECTRUMT'][12], value['SPECTRUMT'][13], value['SPECTRUMT'][14],
                            value['SPECTRUMT'][15],
                            value['SPECTRUMT'][16], value['SPECTRUMT'][17], value['SPECTRUMT'][18],
                            value['SPECTRUMT'][19],
                            value['SPECTRUMT'][20], value['SPECTRUMT'][21], value['SPECTRUMT'][22],
                            value['SPECTRUMT'][23],
                            value['SPECTRUMT'][24], value['SPECTRUMT'][25], value['SPECTRUMT'][26],
                            value['SPECTRUMT'][27],
                            value['SPECTRUMT'][28], value['SPECTRUMT'][29], value['SPECTRUMT'][30],
                            value['SPECTRUMT'][31],
                            value['SPECTRUMT'][32], value['SPECTRUMT'][33], value['SPECTRUMT'][34],
                            value['SPECTRUMT'][35],
                            value['SPECTRUMT'][36], value['SPECTRUMT'][37], value['SPECTRUMT'][38],
                            value['SPECTRUMT'][39],
                            value['SPECTRUMT'][40], value['SPECTRUMT'][41], value['SPECTRUMT'][42],
                            value['SPECTRUMT'][43],
                            value['SPECTRUMT'][44], bytes_to_int(unix_byte)))
                    conn.commit()

                ############################     LXET     ############################
                cur.execute("SELECT COUNT(*) FROM {}".format(((str(msg_topic))[13:]) + '_AC'))
                table_count = cur.fetchone()[0]

                if table_count == 0:
                    empty_table_LXET = True

                if ("LXET" in value) and (empty_table_LXET == True):
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, LAET, LCET, LZET) VALUES ' \
                                    '(%s,%s,%s,%s,%s) '.format(((str(msg_topic))[13:]) + '_AC')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, value['LXET'][0], value['LXET'][1], value['LXET'][2])
                    cur.execute(insert_script, insert_value)
                    empty_table_LXET = True
                    conn.commit()

                elif ("LXET" in value) and (empty_table_LXET == False):
                    cur.execute(
                        "UPDATE {} SET LAET=%s, LCET=%s, LZET=%s WHERE TimeStamp=%s ".format(
                            ((str(msg_topic))[13:]) + '_AC'),
                        (
                            value['LXET'][0], value['LXET'][1], value['LXET'][2], bytes_to_int(unix_byte)))
                    conn.commit()

                ############################     TEMPORAL     ############################
                cur.execute("SELECT COUNT(*) FROM {}".format(((str(msg_topic))[13:]) + '_AC'))
                table_count = cur.fetchone()[0]

                if table_count == 0:
                    empty_table_TEMPORAL = True

                if ("TEMPORAL" in value) and (empty_table_TEMPORAL == True):
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, LXeqTs, LXeqhh1, LXeqhh2, LXeqhh3) VALUES ' \
                                    '(%s,%s,%s,%s,%s,%s) '.format(((str(msg_topic))[13:]) + '_AC')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, value['TEMPORAL'][0], value['TEMPORAL'][1],
                        value['TEMPORAL'][2], value['TEMPORAL'][3])
                    cur.execute(insert_script, insert_value)
                    empty_table_TEMPORAL = True
                    conn.commit()

                elif ("TEMPORAL" in value) and (empty_table_TEMPORAL == False):
                    cur.execute(
                        "UPDATE {} SET LXeqTs=%s, LXeqhh1=%s, LXeqhh2=%s, LXeqhh3=%s WHERE TimeStamp=%s ".format(
                            ((str(msg_topic))[13:]) + '_AC'),
                        (
                            value['TEMPORAL'][0], value['TEMPORAL'][1], value['TEMPORAL'][2], value['TEMPORAL'][3],
                            bytes_to_int(unix_byte)))
                    conn.commit()
                if len(msg) == 2:
                    print("Battery level : " + str(decode_battery_percentage(msg[0:1])))
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, Battery) VALUES (%s,%s,%s)'.format(
                        ((str(msg_topic))[13:]) + '_Battery')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, decode_battery_percentage(msg[0:1]))
                    cur.execute(insert_script, insert_value)

                    conn.commit()
                    msg = msg[1:]

                # if len(msg) == 5:
                #     print("Battery level : " + str(decode_battery_percentage(msg[0:4])))
                #     insert_script = 'INSERT INTO {} (TimeStamp, Topic, Battery) VALUES (%s,%s,%s)'.format(
                #         ((str(msg_topic))[13:]) + '_Battery')
                #     insert_value = (
                #         bytes_to_int(unix_byte), msg_topic, decode_battery_percentage(msg[0:4]))
                #     cur.execute(insert_script, insert_value)
                #
                #     conn.commit()
                #     msg = msg[4:]

                if len(msg) == 1:
                    print("Network status : " + str(decode_network_status(msg[0:1])))
                    insert_script = 'INSERT INTO {} (TimeStamp, Topic, Network) VALUES (%s,%s,%s)'.format(
                        ((str(msg_topic))[13:]) + '_Network')
                    insert_value = (
                        bytes_to_int(unix_byte), msg_topic, round(decode_network_status(msg[0:1])))
                    cur.execute(insert_script, insert_value)

                    conn.commit()
                    msg = msg[1:]

    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def on_message(client, userdata, msg):
    # global c_input_msg
    # global c_output_msg
    # print(c_output_msg, c_input_msg)
    # c_input_msg += 1
    index_compress = bytes_to_int(msg.payload[0:1])
    if index_compress == 1:
        gzip_header = b'\x1f\x8b\x08\x00\xfej\xc6c\x02\xff'
        msg_uncompressed = gzip.decompress(gzip_header + msg.payload[1:])
        split = msg_uncompressed.split(b'\xff')
        for s in split[1:]:
            decode_msg(s, msg.topic)
        # c_output_msg += 1


def sub_client():
    sslSettings = ssl.SSLContext(mqtt.client.ssl.PROTOCOL_TLSv1_2)
    auth = {'username': user, 'password': pwd}
    subscribe.callback(on_message, "#", hostname=hostname,
                       port=port, auth=auth, tls=sslSettings, protocol=paho.MQTTv31)


def int_to_bytes(x, n_bite):
    """
    Convert integer to hexadecimal
    x : integer
    n_bite : the size in bite
    return : hexadecimal
    """
    return x.to_bytes(n_bite, 'big')


if __name__ == "__main__":
    sub_client()
    # msg = b'\x00c\xc7\xb3\x07\x08\x01@\x01@\x01@\x08\x01@\x01@\x01@\x08\x01@\x01@\x01@\x08\x01@\x01@\x01@\x08\x01@\x01@\x01@\x08\x01@\x01@\x01@\x08\x01@\x01@\x01@'
    # decode_msg(msg)

    # ---------------- DROP ALL TABLES ----------------#
    # DO $$
    # DECLARE
    #   table_name text;
    # BEGIN
    #     FOR table_name IN(SELECT tablename FROM pg_tables WHERE tablename LIKE 'sn%') LOOP
    #         EXECUTE 'DROP TABLE IF EXISTS ' || table_name || ' CASCADE';
    #     END LOOP;
    # END $$;
