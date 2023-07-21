package eu.mchv.telegrambackupdaemon.data;

import java.util.List;

public record FormattedText(String text, List<TextEntity> entities) {}
