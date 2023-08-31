package org.fasteam.sdk.core.conf;

import org.apache.hadoop.shaded.org.apache.commons.configuration2.Configuration;
import org.apache.hadoop.shaded.org.apache.commons.configuration2.XMLConfiguration;
import org.apache.hadoop.shaded.org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.hadoop.shaded.org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.shaded.org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;

import javax.management.InstanceNotFoundException;


/**
 * Description:  org.fasteam.sdk.core.conf
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/6/27
 */
public class Config {
    private static Config instance;
    public static Configuration props;
    public Config(ConfigProvider provider) throws ConfigurationException {
        if(instance==null){
            instance = this;
            loadProps(provider);
        }
    }
    protected synchronized Configuration loadProps(ConfigProvider provider) throws ConfigurationException {
        if (props == null) {
            if(ConfigProvider.REMOTE==provider){
                XMLConfiguration config = new Configurations().xml(provider.getUrl());
//                XPathExpressionEngine xpathEngine = new XPathExpressionEngine();
//                config.setExpressionEngine(xpathEngine);
                props = config;
            }else if(ConfigProvider.LOCAL==provider){
                props = new Configurations().xml("default-rules.xml");
            }
        }
        return props;
    }

    public static Config getInstance() throws InstanceNotFoundException {
        if(instance==null){
            throw new InstanceNotFoundException();
        }
        return instance;
    }

}
