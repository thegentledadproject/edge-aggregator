# polymarket_source.py
"""Live Polymarket CLOB data source: per-station market/token registry + REST client."""
import requests

CLOB_BASE_URL = "https://clob.polymarket.com"

# Maps each weather station to the Polymarket temperature-bucket markets that
# track it. Each entry's token_id is the real CLOB token for that bucket's
# "Yes" outcome, pulled live from the Gamma API
# (https://gamma-api.polymarket.com/public-search) for the "Highest temperature
# in <city>" markets covering 2026-07-07.
#
# These are daily markets — Polymarket opens a new condition (and therefore
# new token_ids) for each date. This snapshot will go stale once 2026-07-07's
# markets resolve; a production deployment should re-query the Gamma API each
# day (e.g. via public-search for "highest temperature in <city>") to refresh
# this registry with the current day's token_ids instead of hardcoding them.
STATION_MARKETS = {
    "KORD": [  # highest-temperature-in-chicago-on-july-7-2026
        {"bucket": "73°F or below", "token_id": "26291223424109585600207934202230026357474763635344920167046344764904148110773"},
        {"bucket": "74-75°F", "token_id": "100895827894875499978632092488619103359809174180365390857614122878265552476102"},
        {"bucket": "76-77°F", "token_id": "82203052229389900369359990559137707684026737742354220772173703219919758645056"},
        {"bucket": "78-79°F", "token_id": "50845923506716350905474927029414032210346480002839404335554750127718649786088"},
        {"bucket": "80-81°F", "token_id": "112448117970095186753625555682500003994341940465174524112175377472957763672200"},
        {"bucket": "82-83°F", "token_id": "103643464667135198055012637562335803520985389509917548584464041114617146492799"},
        {"bucket": "84-85°F", "token_id": "115648523236384549138956306779174143916313465750614611481668626267928317937440"},
        {"bucket": "86-87°F", "token_id": "20100977785247184422469381485100823743019427370067498842185221346177113696583"},
        {"bucket": "88-89°F", "token_id": "105865813564955015740283823848904904357234782539958528755172323222851465899239"},
        {"bucket": "90-91°F", "token_id": "27691245490388884534654184876318314448815613403836578439787571183455205773443"},
        {"bucket": "92°F or higher", "token_id": "30492093831832262696127000385002842749281252046374806446238237380576725182355"},
    ],
    "KNYC": [  # highest-temperature-in-nyc-on-july-7-2026
        {"bucket": "65°F or below", "token_id": "11058905746533008824884402703057479002725727836307429872624192461808207736666"},
        {"bucket": "66-67°F", "token_id": "48158650777526707170787853580429521651483497330164601646904096461499748465257"},
        {"bucket": "68-69°F", "token_id": "83653866258090171605553978679542842116140925906480670423416075202160415493721"},
        {"bucket": "70-71°F", "token_id": "86412613839970606425076403433218198812945355111726945530373773259644730572181"},
        {"bucket": "72-73°F", "token_id": "55142487752353300564167362764431404016557900280620226582979643037970073541880"},
        {"bucket": "74-75°F", "token_id": "106067757882781728580607923577441075480215670655348381001227396149275561659934"},
        {"bucket": "76-77°F", "token_id": "59110317043925294694573519591021808656257749856405647695029634637452076079388"},
        {"bucket": "78-79°F", "token_id": "71767216510055536357977707217258050040386093415526436764261418977400276544075"},
        {"bucket": "80-81°F", "token_id": "11033999829952018748807170338617083381271383291699813924985382147478195412665"},
        {"bucket": "82-83°F", "token_id": "108977898785680056265303408160554723993273400092313091195202599762096983064608"},
        {"bucket": "84°F or higher", "token_id": "115669947678572400005461977179963683020827997791748881041127878176752314327843"},
    ],
    "KAUS": [  # highest-temperature-in-austin-on-july-7-2026
        {"bucket": "85°F or below", "token_id": "11755717127364877395276395794859926685582379780082967337709147829442797038722"},
        {"bucket": "86-87°F", "token_id": "67657027908699285019011389994999809124144686514251386349592795402000941261873"},
        {"bucket": "88-89°F", "token_id": "46354699110126944800502130632659014773346159991806769507935369323854904485720"},
        {"bucket": "90-91°F", "token_id": "78817453181689144262209503223123409774021650144185412169854759353369881524059"},
        {"bucket": "92-93°F", "token_id": "6437252720563493494849821723099764962413115433240058960360451660464807992261"},
        {"bucket": "94-95°F", "token_id": "12552999931188170225800294745160468414174528529107583140896148076256683854382"},
        {"bucket": "96-97°F", "token_id": "84256181938946108264221522806665803561190150300152596829109059244119167003580"},
        {"bucket": "98-99°F", "token_id": "84322131923001210425357518651334890555169834210594771188539207350143570541504"},
        {"bucket": "100-101°F", "token_id": "103063649339880891199235102021754345893913248773151906388360504323095621376062"},
        {"bucket": "102-103°F", "token_id": "81849079600042920131011852021267937724268196521994849735132393112558578939922"},
        {"bucket": "104°F or higher", "token_id": "57725218087138122087988321645617359296129862013227574857449127253337104255478"},
    ],
}


def fetch_market_prices(station_id: str) -> list:
    """Fetch live midpoint prices for every tracked bucket at a station."""
    if station_id not in STATION_MARKETS:
        raise ValueError(
            f"Unknown station_id {station_id!r}; add its markets to STATION_MARKETS."
        )

    contracts = []
    for contract in STATION_MARKETS[station_id]:
        response = requests.get(
            f"{CLOB_BASE_URL}/midpoint",
            params={"token_id": contract["token_id"]},
            timeout=10,
        )
        response.raise_for_status()
        price = float(response.json()["mid"])
        contracts.append({
            "bucket": contract["bucket"],
            "price": price,
            "token_id": contract["token_id"],
        })
    return contracts
