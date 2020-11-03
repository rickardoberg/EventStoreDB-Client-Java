package com.eventstore.dbclient;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ParseValidConnectionStringTests {
    private final JsonMapper mapper = new JsonMapper();


    @Parameterized.Parameters
    public static Collection validConnectionStrings() {
        return Arrays.asList(new Object[][]{
                {
                        "esdb://localhost",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"hosts\":[{\"address\":\"localhost\",\"port\":2113}]}"
                },
                {
                        "esdb://localhost:2114",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"hosts\":[{\"address\":\"localhost\",\"port\":2114}]}"
                },
                {
                        "esdb://user:pass@localhost:2114",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"defaultCredentials\":{\"login\":\"user\",\"password\":\"pass\"},\"hosts\":[{\"address\":\"localhost\",\"port\":2114}]}"
                },
                {
                        "esdb://user:pass@localhost:2114/",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"defaultCredentials\":{\"login\":\"user\",\"password\":\"pass\"},\"hosts\":[{\"address\":\"localhost\",\"port\":2114}]}"
                },
                {
                        "esdb://user:pass@localhost:2114/?tlsVerifyCert=false",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":false,\"throwOnAppendFailure\":true,\"defaultCredentials\":{\"login\":\"user\",\"password\":\"pass\"},\"hosts\":[{\"address\":\"localhost\",\"port\":2114}]}"
                },
                {
                        "esdb://user:pass@localhost:2114?tlsVerifyCert=false",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":false,\"throwOnAppendFailure\":true,\"defaultCredentials\":{\"login\":\"user\",\"password\":\"pass\"},\"hosts\":[{\"address\":\"localhost\",\"port\":2114}]}"
                },
                {
                        "esdb://user:pass@localhost:2114?tls=false",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":false,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"defaultCredentials\":{\"login\":\"user\",\"password\":\"pass\"},\"hosts\":[{\"address\":\"localhost\",\"port\":2114}]}"
                },
                {
                        "esdb://host1,host2,host3",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"hosts\":[{\"address\":\"host1\",\"port\":2113},{\"address\":\"host2\",\"port\":2113},{\"address\":\"host3\",\"port\":2113}]}"
                },
                {
                        "esdb://host1:1234,host2:4321,host3:3231",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"hosts\":[{\"address\":\"host1\",\"port\":1234},{\"address\":\"host2\",\"port\":4321},{\"address\":\"host3\",\"port\":3231}]}"
                },
                {
                        "esdb://bubaqp2rh41uf5akmj0g-0.mesdb.eventstore.cloud:2113,bubaqp2rh41uf5akmj0g-1.mesdb.eventstore.cloud:2113,bubaqp2rh41uf5akmj0g-2.mesdb.eventstore.cloud:2113",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"hosts\":[{\"address\":\"bubaqp2rh41uf5akmj0g-0.mesdb.eventstore.cloud\",\"port\":2113},{\"address\":\"bubaqp2rh41uf5akmj0g-1.mesdb.eventstore.cloud\",\"port\":2113},{\"address\":\"bubaqp2rh41uf5akmj0g-2.mesdb.eventstore.cloud\",\"port\":2113}]}"
                },
                {
                        "esdb://user:pass@host1:1234,host2:4321,host3:3231?nodePreference=follower",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"follower\",\"tls\":true,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"defaultCredentials\":{\"login\":\"user\",\"password\":\"pass\"},\"hosts\":[{\"address\":\"host1\",\"port\":1234},{\"address\":\"host2\",\"port\":4321},{\"address\":\"host3\",\"port\":3231}]}"
                },
                {
                        "esdb://host1,host2,host3?tls=false",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":false,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"hosts\":[{\"address\":\"host1\",\"port\":2113},{\"address\":\"host2\",\"port\":2113},{\"address\":\"host3\",\"port\":2113}]}"
                },
                {
                        "esdb://127.0.0.1:21573?tls=false",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":false,\"tlsVerifyCert\":true,\"throwOnAppendFailure\":true,\"hosts\":[{\"address\":\"127.0.0.1\",\"port\":21573}]}"
                },
                {
                        "esdb://host1,host2,host3?tlsVerifyCert=false",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"random\",\"tls\":true,\"tlsVerifyCert\":false,\"throwOnAppendFailure\":true,\"hosts\":[{\"address\":\"host1\",\"port\":2113},{\"address\":\"host2\",\"port\":2113},{\"address\":\"host3\",\"port\":2113}]}"
                },
                {
                        "esdb+discover://user:pass@host?nodePreference=follower&tlsVerifyCert=false",
                        "{\"dnsDiscover\":true,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"follower\",\"tls\":true,\"tlsVerifyCert\":false,\"throwOnAppendFailure\":true,\"defaultCredentials\":{\"login\":\"user\",\"password\":\"pass\"},\"hosts\":[{\"address\":\"host\",\"port\":2113}]}"
                },
                {
                        "esdb://my%3Agreat%40username:UyeXx8%24%5EPsOo4jG88FlCauR1Coz25q@host?nodePreference=follower&tlsVerifyCert=false",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":3,\"discoveryInterval\":500,\"gossipTimeout\":3000,\"nodePreference\":\"follower\",\"tls\":true,\"tlsVerifyCert\":false,\"throwOnAppendFailure\":true,\"defaultCredentials\":{\"login\":\"my:great@username\",\"password\":\"UyeXx8$^PsOo4jG88FlCauR1Coz25q\"},\"hosts\":[{\"address\":\"host\",\"port\":2113}]}"
                },
                {
                        "esdb://host?maxDiscoverAttempts=200&discoveryInterval=1000&gossipTimeout=1&nodePreference=leader&tls=false&tlsVerifyCert=false&throwOnAppendFailure=false",
                        "{\"dnsDiscover\":false,\"maxDiscoverAttempts\":200,\"discoveryInterval\":1000,\"gossipTimeout\":1,\"nodePreference\":\"leader\",\"tls\":false,\"tlsVerifyCert\":false,\"throwOnAppendFailure\":false,\"hosts\":[{\"address\":\"host\",\"port\":2113}]}"
                }
        });
    }

    public void assertEquals(ConnectionSettings settings, ConnectionSettings other) {
        Assert.assertEquals(settings.dnsDiscover, other.dnsDiscover);
        Assert.assertEquals(settings.maxDiscoverAttempts, other.maxDiscoverAttempts);
        Assert.assertEquals(settings.discoveryInterval, other.discoveryInterval);
        Assert.assertEquals(settings.gossipTimeout, other.gossipTimeout);
        Assert.assertEquals(settings.nodePreference, other.nodePreference);
        Assert.assertEquals(settings.tls, other.tls);
        Assert.assertEquals(settings.tlsVerifyCert, other.tlsVerifyCert);
        Assert.assertEquals(settings.throwOnAppendFailure, other.throwOnAppendFailure);

        Assert.assertEquals(settings.hosts.length, other.hosts.length);
        IntStream.range(0, settings.hosts.length).forEach((i) -> {
            Assert.assertEquals(settings.hosts[i].getHostname(), other.hosts[i].getHostname());
            Assert.assertEquals(settings.hosts[i].getPort(), other.hosts[i].getPort());
        });
    }

    @Parameterized.Parameter
    public String connectionString;

    @Parameterized.Parameter(1)
    public String jsonSettings;

    @Test
    public void test() throws ParseError, JsonProcessingException {

        ConnectionSettings expectedSettings = this.parseJson(jsonSettings);
        ConnectionSettings parsedSettings = ConnectionString.parse(connectionString);

        this.assertEquals(expectedSettings, parsedSettings);
    }

    private ConnectionSettings parseJson(String input) throws JsonProcessingException {
        ConnectionSettingsBuilder builder = ConnectionSettings.builder();
        JsonNode tree = mapper.readTree(input);

        builder
                .dnsDiscover(tree.get("dnsDiscover").asBoolean())
                .maxDiscoverAttempts(tree.get("maxDiscoverAttempts").asInt())
                .discoveryInterval(tree.get("discoveryInterval").asInt())
                .gossipTimeout(tree.get("gossipTimeout").asInt())
                .nodePreference(NodePreference.valueOf(tree.get("nodePreference").asText().toUpperCase()))
                .tls(tree.get("tls").asBoolean())
                .tlsVerifyCert(tree.get("tlsVerifyCert").asBoolean())
                .throwOnAppendFailure(tree.get("throwOnAppendFailure").asBoolean());


        tree.get("hosts").elements().forEachRemaining((host) -> {
            builder.addHost(new Endpoint(host.get("address").asText(), host.get("port").asInt()));
        });


        return builder.buildConnectionSettings();
    }
}
