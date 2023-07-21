package eu.mchv.telegrambackupdaemon.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import eu.mchv.telegrambackupdaemon.data.File.Audio;
import eu.mchv.telegrambackupdaemon.data.File.Document;
import eu.mchv.telegrambackupdaemon.data.File.Photo;
import eu.mchv.telegrambackupdaemon.data.File.Video;
import java.time.Duration;
import org.jetbrains.annotations.Nullable;

@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
		@Type(value = Photo.class, name = "photo"),
		@Type(value = Video.class, name = "video"),
		@Type(value = Audio.class, name = "audio"),
		@Type(value = Document.class, name = "document")
})
public sealed interface File {

	record Photo(long localId, @Nullable Integer width, @Nullable Integer height) implements File {}

	record Video(long localId, @Nullable Integer width, @Nullable Integer height,
							 @Nullable Duration duration) implements File {}

	record Audio(long localId, @Nullable Duration duration) implements File {}

	record Document(long localId, @Nullable String mime, @Nullable String name) implements File {}

	long localId();
}
