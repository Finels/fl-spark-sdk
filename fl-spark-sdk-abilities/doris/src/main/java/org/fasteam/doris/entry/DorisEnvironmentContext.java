package org.fasteam.doris.entry;

import lombok.SneakyThrows;
import org.fasteam.sdk.core.EnvironmentContext;
import org.fasteam.sdk.core.conf.Config;
import static org.fasteam.doris.entry.DorisEnvironmentParameter.*;

/**
 * Description:  PACKAGE_NAME
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/9/15
 */
public class DorisEnvironmentContext extends EnvironmentContext<DorisEnvironmentParameter> {
    @SneakyThrows
    @Override
    public void initialization() {
        Config config = Config.getInstance();
        String host = config.props.get(String.class,HOST);
        String user = config.props.get(String.class,USERNAME);
        String password = config.props.get(String.class,PASSWORD);
        setFeature(DorisEnvironmentParameter.builder().uri(host).username(user).password(password).build());
        super.initialization();
    }

    @Override
    public DorisEnvironmentParameter callbackFeature() {
        return getFeature();
    }
}
