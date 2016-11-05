package org.elasticsearch.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class MockNode extends Node {

    @SuppressWarnings("unchecked")
    public MockNode(Settings settings) {
        this(settings, Collections.EMPTY_LIST);
    }
    public MockNode(Settings settings, Class<? extends Plugin> classpathPlugin) {
        this(settings, list(classpathPlugin));
    }

    public MockNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
        super(InternalSettingsPreparer.prepareEnvironment(settings, null), Version.CURRENT, classpathPlugins);
    }

    private static Collection<Class<? extends Plugin>> list(Class<? extends Plugin> classpathPlugin) {
        Collection<Class<? extends Plugin>> list = new ArrayList<>();
        list.add(classpathPlugin);
        return list;
    }

}
