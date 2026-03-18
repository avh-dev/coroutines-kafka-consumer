package avh.ckc.core

/**
 * Marks declarations that are intentionally more visible than necessary
 * for production code, solely to support testing.
 *
 * This annotation is a documentation marker (no runtime effect).
 */
@Retention(AnnotationRetention.SOURCE)
@Target(
    AnnotationTarget.CLASS,
    AnnotationTarget.FUNCTION,
    AnnotationTarget.PROPERTY,
    AnnotationTarget.FIELD,
    AnnotationTarget.CONSTRUCTOR
)
internal annotation class VisibleForTesting