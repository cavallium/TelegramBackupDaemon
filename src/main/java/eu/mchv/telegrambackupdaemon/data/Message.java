package eu.mchv.telegrambackupdaemon.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import eu.mchv.telegrambackupdaemon.data.Message.MessagePhoto;
import eu.mchv.telegrambackupdaemon.data.Message.MessageText;
import java.util.Optional;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
		@Type(value = MessageText.class, name = "text"),
		@Type(value = MessagePhoto.class, name = "photo")
})
public sealed interface Message {

	record MessageText(long chatId, long senderId, @Nullable FormattedText text) implements Message {

		@Override
		public Optional<FormattedText> getText() {
			return Optional.ofNullable(text);
		}
	}

	record MessagePhoto(long chatId, long senderId, File media, @Nullable FormattedText caption) implements Message {

		@Override
		public Optional<FormattedText> getText() {
			return Optional.ofNullable(caption);
		}

		@Override
		public Stream<File> getMedia() {
			return Message.super.getMedia();
		}
	}

	long chatId();

	long senderId();

	default Optional<FormattedText> getText() {
		return Optional.empty();
	}

	default Stream<File> getMedia() {
		return Stream.of();
	}
}
