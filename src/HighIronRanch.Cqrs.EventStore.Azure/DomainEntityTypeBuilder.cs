using System;
using System.Linq;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class DomainEntityTypeBuilder : IDomainEntityTypeBuilder
    {
        public Type Build(string typeName)
        {
            Type type;
            var assemblies = AppDomain.CurrentDomain.GetAssemblies();
            var firstDotIndex = typeName.IndexOf('.');

            if (firstDotIndex > 0)
            {
                // the type most likely lives inside an assembly with a similar namespace
                // by looking at these first, we can skip any external assemblies

                var firstNamespacePart = typeName.Substring(0, firstDotIndex);
                foreach (var assembly in assemblies.Where(assembly => assembly.FullName.StartsWith(firstNamespacePart)))
                {
                    type = assembly.GetType(typeName);

                    if (type != null)
                        return type;
                }
            }

            // we were unable to find the type in an assembly with a similar namespace
            // as a fallback, scan every assembly to make a type match
            foreach (var assembly in assemblies)
            {
                type = assembly.GetType(typeName);
                if (type != null)
                    return type;
            }

            return null;
        }
    }
}