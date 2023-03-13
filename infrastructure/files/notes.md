if pContext, ok := app.contexts[context]; ok {
    if pContext.reference == nil {
        str := fmt.Sprintf("there is zero (0) ContentKey in the given context: %d", context)
        return errors.New(str)
    }

    return app.contexts[context].reference.ContentKeys().Erase(hash)
}

str := fmt.Sprintf("the given context (%d) does not exists and therefore the resource cannot be deleted by hash", context)
return errors.New(str)
